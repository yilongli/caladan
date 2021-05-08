#include "shuffle_udp.h"

extern "C" {
#include <base/log.h>
#include <base/cpu.h>
#include <net/ip.h>
#include <runtime/smalloc.h>
#include <runtime/timetrace.h>
#include <runtime/thread.h>
#include <runtime/ustats.h>
}

#include <deque>
#include <memory>
#include <random>
#include <algorithm>
#include "QueueEstimator.h"

#include "thread.h"
#include "timer.h"

namespace {
    inline void
    tt_record(const char* format,
            uint64_t arg0 = 0, uint64_t arg1 = 0, uint64_t arg2 = 0,
            uint64_t arg3 = 0)
    {
        // FIXME: well, strictly speaking, we must disable preemption during tt;
        // but that requires two very expensive full barriers and adds 10 ns overhead.
        // Perhaps a better approach is to integrate the switch of tt_buf
        // into the scheduler? but what if a preemption occurs in the middle of
        // tt_record_buf? may be use an atomic FAA instr to occupy a slot?
//        tt_record4_np(format, arg0, arg1, arg2, arg3);
        tt_record4(cpu_get_current(), format, arg0, arg1, arg2, arg3);
//        log_info(format, arg0, arg1, arg2, arg3);
    }

    inline void
    tt_record(uint64_t tsc, const char* format,
            uint64_t arg0 = 0, uint64_t arg1 = 0, uint64_t arg2 = 0,
            uint64_t arg3 = 0)
    {
        tt_record4_tsc(cpu_get_current(), tsc, format, arg0, arg1, arg2, arg3);
//        log_info(format, arg0, arg1, arg2, arg3);
    }
}

#define ENABLE_TIMESTAMP 1
#if ENABLE_TIMESTAMP
#define timestamp_decl(x) \
    uint64_t timestamp_get(x) = 0;
#define timestamp_create(x) \
    uint64_t timestamp_get(x) = rdtsc();
#define timestamp_update(x) \
    timestamp_get(x) = rdtsc();
#define timestamp_get(x) _timestamp_##x
#else
#define timestamp_decl(x)
#define timestamp_create(x)
#define timestamp_update(x)
#define timestamp_get(x) 0
#endif

DEFINE_PERCPU_METRIC(grpt_msg_cycles);
DEFINE_PERCPU_METRIC(tx_data_cycles);
DEFINE_PERCPU_METRIC(tx_ack_cycles);
DEFINE_PERCPU_METRIC(handle_ack_cycles);
DEFINE_PERCPU_METRIC(handle_data_cycles);
DEFINE_PERCPU_METRIC(rx_data_pkts);
DEFINE_PERCPU_METRIC(rx_ack_pkts);
DEFINE_PERCPU_METRIC(tx_data_pkts);
DEFINE_PERCPU_METRIC(tx_ack_pkts);

// TODO: explain why we need this macro to maintain per-cpu metric efficiently
#define percpu_metric_scoped_cs(metric, mutex, start) \
    rt::Mutex* _m = &mutex;                     \
    rt::ScopedLock<rt::Mutex> _scoped_lock;     \
    bool _success = _m->TryLock();              \
    if (unlikely(!_success)) {                  \
        timestamp_create(mutex_lock)            \
        percpu_metric_get(metric) +=            \
                timestamp_get(mutex_lock) - timestamp_get(start); \
        _scoped_lock.construct(_m, false);      \
        timestamp_update(start)                 \
    } else {                                    \
        _scoped_lock.construct(_m, true);       \
    }


/// Maximum number of bytes that can fit in a UDP-based shuffle message.
static const size_t MAX_PAYLOAD = UDP_MAX_PAYLOAD - sizeof(udp_shuffle_msg_hdr);

/// Size of the UDP and IP headers, in bytes.
static size_t UDP_IP_HDR_SIZE = sizeof(udp_hdr) + sizeof(ip_hdr);

/// Declared in the global scope to enable access from both udp_shuffle_init()
/// and udp_shuffle().
static udpspawner_t* udp_spawner;

/**
 * Keep track of the progress of an outbound message.
 */
struct udp_out_msg {

    /// Rank of the message receiver.
    const int peer;

    /// Total number of packets in this message.
    const size_t num_pkts;

    /// A 64-bit atomic variable which describes the state of the sliding send
    /// window (i.e., the range of packets the sender is allowed transmit).
    /// This atomic variable is actually composed from two 32-bit numbers to
    /// enable more efficient accesses. The higher 32 bits represent the start
    /// index of the send window (i.e., the first packet in the message which
    /// has not been acknowledged by the receiver). The lower 32 bits represent
    /// the total number of packets the sender is allowed to send (i.e., index
    /// of the first packet the sender isn't allowed to send).
    std::atomic<uint64_t> send_wnd;

    /// Index of the next packet to send.
    size_t next_send_pkt;

    /// Last time this message received an ACK, in rdtsc ticks.
    uint64_t last_ack_tsc;

    /// Used to chain this outbound message into the send queue.
    struct list_node q_link;

    explicit udp_out_msg(int peer, size_t num_pkts, uint32_t unsched_pkts)
        : peer(peer)
        , num_pkts(num_pkts)
        , send_wnd(unsched_pkts)
        , next_send_pkt()
        , last_ack_tsc()
        , q_link()
    {}

    size_t remaining_pkts() const {
        return num_pkts - next_send_pkt;
    }

    double remaining_frac() const {
        return num_pkts ?
            static_cast<double>(num_pkts - next_send_pkt) / num_pkts : 0;
    }

    static int compare_grpt(const udp_out_msg& x, const udp_out_msg& y)
    {
        return (int) x.remaining_pkts() - (int) y.remaining_pkts();
    }

    static int compare_grpf(const udp_out_msg& x, const udp_out_msg& y)
    {
        double v = x.remaining_frac() - y.remaining_frac();
        if (v < 0) {
            return -1;
        } else if (v > 0) {
            return 1;
        } else {
            return 0;
        }
    }

    static int compare_srpt(const udp_out_msg& x, const udp_out_msg& y)
    {
        return -1 * compare_grpt(x, y);
    }
};

/**
 * Keep track of the progress of an inbound message.
 */
struct udp_in_msg {
    /// Network address of the message sender.
    const netaddr remote_addr;

    /// Rank of the message sender in the cluster.
    const int peer;

    /// Number of packets in the message.
    size_t num_pkts;

    /// Total number of packets that have been granted (including unscheduled
    /// packets). Only used by the grantor thread.
    size_t num_granted;

    /// Protects @recv_wnd_start and @recv_wnd.
    rt::Mutex mutex;

    /// Start index of the receive window. This is the first packet in the
    /// message which has not been received.
    size_t recv_wnd_start;

    /// Status of the packets in the receive window (the value indicates if
    /// the packet has been received). The entries in the window are organized
    /// as a ring, where the status of packet @recv_wnd_start is stored at index
    /// @recv_wnd_start mod @recv_wnd::size.
    std::vector<bool> recv_wnd;

    /// Constant size of @recv_wnd.
    const size_t recv_wnd_size;

    /// True if the corresponding shfl_msg_buf struct has been initialized.
    std::atomic_bool buf_inited;

    /// Used to chain this inbound message into the grant queue.
    struct list_node q_link;

    explicit udp_in_msg(netaddr raddr, int peer, size_t max_send_wnd)
        : remote_addr(raddr)
        , peer(peer)
        , num_pkts()
        , num_granted()
        , mutex()
        , recv_wnd_start()
        , recv_wnd(max_send_wnd, false)
        , recv_wnd_size(recv_wnd.size())
        , buf_inited()
        , q_link()
    {}

    /// Return the number of packets that have not been granted.
    size_t ungranted_pkts() const {
        return num_pkts - num_granted;
    }

    double ungranted_frac() const {
        return ungranted_pkts() * 1.0 / num_pkts;
    }

    static int compare_grpt(const udp_in_msg& x, const udp_in_msg& y)
    {
        return (int) x.ungranted_pkts() - (int) y.ungranted_pkts();
    }

    static int compare_grpf(const udp_in_msg& x, const udp_in_msg& y)
    {
        double v = x.ungranted_frac() - y.ungranted_frac();
        if (v < 0) {
            return -1;
        } else if (v > 0) {
            return 1;
        } else {
            return 0;
        }
    }

    static int compare_srpt(const udp_in_msg& x, const udp_in_msg& y)
    {
        return -1 * compare_grpt(x, y);
    }
};

struct udp_shuffle_op {

    shuffle_op* common_op;

    const int local_rank;

    const int num_nodes;

    /// Local network address to use in this shuffle operation.
    const netaddr local_addr;

    /// Maximum # bytes carried in a UDP packet (must be <= MAX_PAYLOAD).
    const size_t max_payload;

    /// Skip memcpy in @rx_thread?
    const bool no_rx_memcpy;

    /// Round-trip bytes.
    const size_t rtt_bytes;

    /// Maximum size of the sliding window which controls the number of packets
    /// the sender is allowed to send ahead of the last acknowledged packet.
    const size_t max_send_wnd;

    /// Minimum number of packets granted to each message at once; used to
    /// reduce the number of ACK packets.
    const size_t min_ack_inc;

    /// Scheduling policy.
    const ShufflePolicy policy;

    /// Number of data packets of a message the sender is allowed to send
    /// without getting grants from the receiver.
    const size_t unsched_pkts;

    /// Keeps track of the status of each outbound message; shared between
    /// the TX and RX threads. No need to use a mutex for the containers since
    /// only the message states are mutable.
    std::deque<udp_out_msg> tx_msgs;

    std::deque<udp_in_msg> rx_msgs;

    /// Number of outbound messages that have been fully acknowledged by the
    /// remote peers.
    std::atomic<int> acked_msgs;

    /// Number of inbound messages that haven't been fully received.
    std::atomic<int> incompleted_in_msgs;

    /// Number of grants that can be issued by the receiver (may drop below zero
    /// temporarily).
    std::atomic<int> grants_avail;

    /// Protects @grant_queue.
    rt::Mutex grant_mutex;

    /// Priority queue containing all incoming messages that haven't been fully
    /// granted. The order of the messages depends on the granting policy.
    /// Note: the queue is only accessed by the grantor thread normally but it
    /// must be populated by the receive threads first; as a result, the queue
    /// must be protected by @grant_mutex.
    struct list_head grant_queue;

    /// Protects @send_queue.
    rt::Mutex send_mutex;

    /// Priority queue containing all outbound messages that haven't been fully
    /// transmitted.
    struct list_head send_queue;

    /// Semaphore used to park the TX thread when all bytes in the sliding
    /// windows of the outbound messages have been transmitted (it can be
    /// woken up by the RX thread later when ACKs arrive).
    rt::Semaphore send_ready;

    /// Index of the next message to pull in @pull_targets.
    std::atomic<size_t> next_pull_id;

    /// Target nodes from which to pull our data; only used in pull-based
    /// policies such as Hadoop.
    std::vector<udp_in_msg*> pull_targets;

    /// Number of pull requests we've received so far.
    std::atomic<int> pull_reqs;

    rt::Semaphore shuffle_done;

    explicit udp_shuffle_op(Cluster* c, shuffle_op* common_op,
            RunBenchOptions* opts)
        : common_op(common_op)
        , local_rank(c->local_rank)
        , num_nodes(c->num_nodes)
        , local_addr({c->local_ip, opts->udp_port})
        , max_payload(opts->max_payload ?
                std::min(MAX_PAYLOAD, opts->max_payload) : MAX_PAYLOAD)
        , no_rx_memcpy(opts->no_rx_memcpy)
        // rtt_bytes = bandwdithGbps * 1000 * RTT_us / 8
        , rtt_bytes(opts->link_speed * 1e3 * 12.0 / 8)
        // FIXME: why do we have to over-estimate by 2x?
        , max_send_wnd(2 * (rtt_bytes + max_payload - 1) / max_payload)
        , min_ack_inc(5)
        , policy(opts->policy)
        , unsched_pkts(opts->unsched_pkts < 0 ? max_send_wnd :
                (size_t) opts->unsched_pkts)
        , tx_msgs()
        , rx_msgs()
        , acked_msgs(1)
        , incompleted_in_msgs(num_nodes - 1)
        , grants_avail(int(max_send_wnd * opts->over_commit))
        , grant_mutex()
        , grant_queue(LIST_HEAD_INIT(grant_queue))
        , send_mutex()
        , send_queue(LIST_HEAD_INIT(send_queue))
        , send_ready(0)
        , next_pull_id()
        , pull_targets()
        , pull_reqs()
        , shuffle_done(0)
    {
        size_t num_pkts;
        for (int i = 0; i < num_nodes; i++) {
            num_pkts = (i == local_rank) ? 0 :
                    (common_op->out_bufs[i].len + max_payload - 1)/max_payload;
            tx_msgs.emplace_back(i, num_pkts,
                    opts->policy == HADOOP ? 0 : unsched_pkts);
        }

        for (int i = 0; i < num_nodes; i++) {
            netaddr raddr = {c->server_list[i], local_addr.port};
            rx_msgs.emplace_back(raddr, i, max_send_wnd);
        }
        tt_record("udp_shuffle_op: max_send_wnd %u, unsched_pkts %u, "
                "grant_avail %u, min_ack_inc %u", max_send_wnd, unsched_pkts,
                grants_avail.load(), min_ack_inc);

        if (opts->policy == HADOOP) {
            for (auto& in_msg : rx_msgs) {
                if (in_msg.peer != local_rank) {
                    pull_targets.push_back(&in_msg);
                }
            }
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(pull_targets.begin(), pull_targets.end(), g);
        }
    }
};

template <typename T>
static void insert_pq(list_head *pq, T *start, T *new_msg,
        int (*comp)(const T&, const T&))
{
    // If @start is not specified, set @start to be the head of the queue.
    if (!start) {
        start = list_top(pq, T, q_link);
        if (!start) {
            list_add(pq, &new_msg->q_link);
            return;
        }
    }

    // Optimization: if @new_msg is smaller than the last entry, just
    // insert it at the tail of the queue.
    T* end = list_tail(pq, T, q_link);
    if (comp(*new_msg, *end) <= 0) {
        list_add_tail(pq, &new_msg->q_link);
        return;
    }

    // Otherwise, locate the first entry smaller than @new_msg and insert
    // @new_msg right before it.
    while (true) {
        BUG_ON(!start);
        if (comp(*start, *new_msg) < 0) {
            list_add_before(&start->q_link, &new_msg->q_link);
            return;
        }
        start = list_next(pq, start, q_link);
    }
}

/// Insert outgoing message @new_msg to @send_queue based on the shuffle policy.
static void insert_sq(list_head *send_queue, udp_out_msg *new_msg,
        ShufflePolicy policy, udp_out_msg *start = nullptr)
{
    switch (policy) {
        case ShufflePolicy::HADOOP:
            list_add_tail(send_queue, &new_msg->q_link);
            break;
        case ShufflePolicy::SRPT:
            insert_pq(send_queue, (udp_out_msg *)nullptr, new_msg,
                    udp_out_msg::compare_srpt);
            break;
        case ShufflePolicy::GRPT:
            insert_pq(send_queue, start, new_msg, udp_out_msg::compare_grpt);
            break;
        case ShufflePolicy::GRPF:
            insert_pq(send_queue, start, new_msg, udp_out_msg::compare_grpf);
            break;
        default:
            panic("unexpected policy value: %d", policy);
    }
}

/// Insert inbound message @new_msg to @grant_queue based on the shuffle policy.
static void insert_gq(list_head *grant_queue, udp_in_msg *new_msg,
        ShufflePolicy policy, udp_in_msg *start = nullptr)
{
    switch (policy) {
        case ShufflePolicy::HADOOP:
            list_add_tail(grant_queue, &new_msg->q_link);
            break;
        case ShufflePolicy::SRPT:
            insert_pq(grant_queue, (udp_in_msg *)nullptr, new_msg,
                    udp_in_msg::compare_srpt);
            break;
        case ShufflePolicy::GRPT:
            insert_pq(grant_queue, start, new_msg, udp_in_msg::compare_grpt);
            break;
        case ShufflePolicy::GRPF:
            insert_pq(grant_queue, start, new_msg, udp_in_msg::compare_grpf);
            break;
        default:
            panic("unexpected policy value: %d", policy);
    }
}

/**
 * Main function of the grantor thread responsible for sending back ACKs.
 */
static void udp_grantor(Cluster &c, shuffle_op &common_op,
        udp_shuffle_op &op, RunBenchOptions &opts)
{
    udp_shuffle_msg_hdr ack_hdr = {
        .op_id = (int16_t) common_op.id,
        .peer = (int16_t) op.local_rank,
        .ack_no = 0,
    };

    // For pull-based policies, send out initial pull requests to kick start.
    if (opts.policy == HADOOP) {
        size_t n = std::min(opts.max_in_msgs, op.pull_targets.size());
        op.next_pull_id += n;
        for (size_t i = 0; i < n; i++) {
            udp_in_msg* target = op.pull_targets[i];
            ack_hdr.tag = 2;
            ack_hdr.grant_limit = op.unsched_pkts;
            tt_record("node-%d: sending PULL %u to node-%d (grant_limit %u)",
                    op.local_rank, ack_hdr.ack_no, target->peer,
                    ack_hdr.grant_limit);
            udp_send(&ack_hdr, sizeof(ack_hdr), op.local_addr,
                    target->remote_addr);
        }
    }

    while (op.incompleted_in_msgs > 0) {
        // Waking up every 5 us seems to work pretty well in practice.
        rt::Sleep(5);
        if (op.grants_avail.load(std::memory_order_relaxed) <= 0) {
            continue;
        }

        // Find 1st message in the priority queue that can receive more grants.
        udp_in_msg* grantee = nullptr;
        uint16_t ack_no = 0;
        {
            rt::ScopedLock<rt::Mutex> lock_gq(&op.grant_mutex);

            size_t grants_inc = 0;
            udp_in_msg* right_nb = nullptr;
            udp_in_msg* msg;
            list_for_each(&op.grant_queue, msg, q_link) {
                // Hack: read @recv_wnd_start atomically (w/o locking the mutex)
                size_t recv_wnd_start =
                        *((volatile size_t*) &msg->recv_wnd_start);

                // Ensure that allocating more grants to this message won't
                // overflow our receive window.
                size_t outstanding_pkts = msg->num_granted - recv_wnd_start;
                BUG_ON(outstanding_pkts > msg->recv_wnd_size);
                if (outstanding_pkts < msg->recv_wnd_size) {
                    grantee = msg;
                    right_nb = list_next(&op.grant_queue, grantee, q_link);
                    list_del_from(&op.grant_queue, &grantee->q_link);
                    ack_no = recv_wnd_start;
                    grants_inc = std::min(msg->recv_wnd_size - outstanding_pkts,
                            msg->ungranted_pkts());
                    break;
                }
            }
            if (!grantee) {
                continue;
            }

            // Update the message state and put it back to the grant queue if
            // it hasn't been fully granted.
            grants_inc = std::min(grants_inc, op.min_ack_inc);
            grantee->num_granted += grants_inc;
            op.grants_avail.fetch_sub(grants_inc, std::memory_order_relaxed);
            if (grantee->ungranted_pkts() > 0) {
                insert_gq(&op.grant_queue, grantee, op.policy, right_nb);
            }
        }

        // Send back ACKs.
        timestamp_create(send_ack)
        ack_hdr.tag = 1;
        ack_hdr.ack_no = ack_no;
        ack_hdr.grant_limit = grantee->num_granted;
        tt_record("node-%d: sending ACK %u to node-%d (grant_limit %u)",
                op.local_rank, ack_hdr.ack_no, grantee->peer,
                ack_hdr.grant_limit);
        udp_send(&ack_hdr, sizeof(ack_hdr), op.local_addr,
                grantee->remote_addr);
        timestamp_create(end)

        percpu_metric_get(tx_ack_pkts)++;
        percpu_metric_get(tx_ack_cycles) +=
                timestamp_get(end) - timestamp_get(send_ack);
    }
    tt_record("grantor thread done");
}


/**
 * Main function of the RX thread that is responsible for receiving packets,
 * assembling inbound messages, and sending back ACKs.
 */
static void rx_thread(struct udp_spawn_data *d)
{
    auto* op = (udp_shuffle_op*) d->app_state;
    auto* common_op = op->common_op;
    uint64_t start_tsc = rdtsc();
    bool rx_msg_complete = false;

    timestamp_decl(start)
    timestamp_decl(send_ack)
    timestamp_get(start) = start_tsc;

    // Read the message header.
    char* mbuf = (char*) d->buf;
    size_t mbuf_size = d->len;
    auto& msg_hdr = *reinterpret_cast<udp_shuffle_msg_hdr*>(mbuf);
    if (mbuf_size < sizeof(msg_hdr)) {
        panic("unknown mbuf size %ld (expected at least %lu bytes)",
                mbuf_size, sizeof(msg_hdr));
    }

    int peer = msg_hdr.peer;
    if (msg_hdr.op_id != common_op->id) {
        tt_record("node-%d: dropped obsolete packet from op %d, tag %u, "
                  "ack_no %u", msg_hdr.op_id, msg_hdr.tag, msg_hdr.ack_no);
        goto done;
    }

    if (msg_hdr.tag == 1) {
        // ACK message.
        tt_record(start_tsc, "node-%d: received ACK %u from node-%d, grant %u",
                op->local_rank, msg_hdr.ack_no, peer, msg_hdr.grant_limit);
        auto& tx_msg = op->tx_msgs[peer];
//        tx_msg.last_ack_tsc = rdtsc();    // not used; needs lock
        if (msg_hdr.ack_no >= tx_msg.num_pkts) {
            if (op->acked_msgs.fetch_add(1, std::memory_order_relaxed) + 1 ==
                    op->num_nodes) {
                // All outbound messages are acknowledged.
                tt_record("node-%d: all out msgs acknowledged", op->local_rank);
                op->shuffle_done.Up();
            }
        } else {
            uint64_t old_wnd = tx_msg.send_wnd.load();
            uint64_t new_wnd = (uint64_t) msg_hdr.ack_no << 32u |
                    msg_hdr.grant_limit;
            while (new_wnd > old_wnd) {
                if (tx_msg.send_wnd.compare_exchange_strong(old_wnd, new_wnd)) {
                    op->send_ready.Up();
                    break;
                }
            }
        }
    } else if (msg_hdr.tag == 2) {
        // PULL message
        tt_record(start_tsc, "node-%d: received PULL %u from node-%d, grant %u",
                op->local_rank, msg_hdr.ack_no, peer, msg_hdr.grant_limit);
        op->pull_reqs++;
        auto& tx_msg = op->tx_msgs[peer];
        tx_msg.send_wnd = msg_hdr.grant_limit;
        rt::ScopedLock<rt::Mutex> lock_sq(&op->send_mutex);
        list_add_tail(&op->send_queue, &tx_msg.q_link);
        tt_record("node-%d: added message to node-%d from send_queue",
                op->local_rank, tx_msg.peer);
        op->send_ready.Up();
    } else {
        // Normal data segment.
        tt_record(start_tsc, "node-%d: receiving bytes %lu-%lu from node-%d",
                op->local_rank, msg_hdr.start, msg_hdr.start + msg_hdr.seg_size,
                peer);
        // FIXME: need more checks to detect packet corruption
        if (mbuf_size - sizeof(msg_hdr) != msg_hdr.seg_size) {
            panic("unexpected payload size %ld (expected %u)",
                    mbuf_size - sizeof(msg_hdr), msg_hdr.seg_size);
        }

        // Initialize the memory buffer to hold the inbound message.
        size_t pkt_idx = msg_hdr.start / op->max_payload;
        udp_in_msg& rx_msg = op->rx_msgs[peer];
        char* buf;
        if (unlikely(!rx_msg.buf_inited.load(std::memory_order_acquire))) {
            percpu_metric_scoped_cs(handle_data_cycles, rx_msg.mutex, start)
            if (!rx_msg.buf_inited.load(std::memory_order_acquire)) {
                buf = common_op->next_inmsg_addr.fetch_add(msg_hdr.msg_size,
                        std::memory_order_relaxed);
                common_op->in_bufs[peer].addr = buf;
                common_op->in_bufs[peer].len = msg_hdr.msg_size;
                rx_msg.num_pkts = (msg_hdr.msg_size + op->max_payload - 1) /
                                  op->max_payload;
                rx_msg.num_granted = std::min(op->unsched_pkts,
                        rx_msg.num_pkts);
                rx_msg.buf_inited.store(true, std::memory_order_release);

                // Populate the grant queue.
                if (rx_msg.num_pkts > op->unsched_pkts) {
                    rt::ScopedLock<rt::Mutex> lock_gq(&op->grant_mutex);
                    insert_gq(&op->grant_queue, &rx_msg, op->policy);
                }
            }
        }
        buf = common_op->in_bufs[peer].addr;

        // Refill the grants if this packet was granted by us eariler.
        if (pkt_idx >= op->unsched_pkts) {
            op->grants_avail.fetch_add(1, std::memory_order_relaxed);
        }

        // Read the shuffle payload.
        if (!op->no_rx_memcpy) {
            memcpy(buf + msg_hdr.start, mbuf + sizeof(msg_hdr),
                    msg_hdr.seg_size);
        }

        // Attempt to advance the sliding window.
        {
            percpu_metric_scoped_cs(handle_data_cycles, rx_msg.mutex, start)
            rx_msg.recv_wnd[pkt_idx % rx_msg.recv_wnd_size] = true;
            while (true) {
                size_t idx = rx_msg.recv_wnd_start % rx_msg.recv_wnd_size;
                if (!rx_msg.recv_wnd[idx]) {
                    break;
                }
                rx_msg.recv_wnd[idx] = false;
                rx_msg.recv_wnd_start++;
            }
            rx_msg_complete = (rx_msg.recv_wnd_start >= rx_msg.num_pkts);
        }

        if (rx_msg_complete) {
            tt_record("node-%d: received message from node-%d",
                    op->local_rank, peer);
            if (op->incompleted_in_msgs.fetch_sub(1) == 1) {
                // All inbound messages are received.
                tt_record("node-%d: all in msgs received", op->local_rank);
                op->shuffle_done.Up();
            }

            // Send back an ACK.
            timestamp_update(send_ack)
            udp_shuffle_msg_hdr ack_hdr = {
                .op_id = msg_hdr.op_id,
                .peer = (int16_t) op->local_rank,
                .tag = 1,
                .ack_no = (uint16_t) rx_msg.num_pkts,
                .grant_limit = (uint16_t) rx_msg.num_pkts,
//                .seg_size = 0, .msg_size = 0, .start = 0,
            };
            tt_record("node-%d: sending ACK %u to node-%d", op->local_rank,
                    ack_hdr.ack_no, peer);
            udp_respond(&ack_hdr, sizeof(ack_hdr), d);
            percpu_metric_get(tx_ack_pkts)++;

            // Send out another pull request.
            if (op->policy == HADOOP) {
                size_t idx = op->next_pull_id.fetch_add(1);
                if (idx < op->pull_targets.size()) {
                    ack_hdr.tag = 2;
                    ack_hdr.ack_no = 0;
                    ack_hdr.grant_limit = op->unsched_pkts;
                    tt_record("node-%d: sending PULL %u to node-%d",
                            op->local_rank, ack_hdr.ack_no, peer);
                    udp_send(&ack_hdr, sizeof(ack_hdr), op->local_addr,
                            op->pull_targets[idx]->remote_addr);
                }
            }
        }
    }

  done:
    udp_spawn_data_release(d->release_data);
    timestamp_create(end)
    if (msg_hdr.tag == 1) {
        percpu_metric_get(rx_ack_pkts)++;
        percpu_metric_get(handle_ack_cycles) +=
                timestamp_get(end) - timestamp_get(start);
    } else {
        if (!rx_msg_complete) {
            timestamp_get(send_ack) = timestamp_get(end);
        }
        percpu_metric_get(rx_data_pkts)++;
        percpu_metric_get(handle_data_cycles) +=
                timestamp_get(send_ack) - timestamp_get(start);
        percpu_metric_get(tx_ack_cycles) +=
                timestamp_get(end) - timestamp_get(send_ack);
    }
}

/**
 * Main function of the TX thread that is responsible for scheduling outbound
 * messages, enforcing flow control (using a sliding window), and retransmitting
 * packets on timeouts.
 *
 * \param c
 *      Cluster object.
 * \param common_op
 *      Shuffle object that keeps track of the progress.
 * \param udp_op
 *      UDP-specifc shuffle object.
 */
void
udp_tx_main(Cluster& c, shuffle_op& common_op, udp_shuffle_op& op,
        RunBenchOptions& opts)
{
    uint64_t idle_cyc;
    uint64_t now;

    timestamp_create(start)
    idle_cyc = 0;

    // Ranks of the remote peers to communicate with (in desired order).
    std::vector<int> peers;
    peers.reserve(c.num_nodes - 1);
    for (int i = 1; i < c.num_nodes; i++) {
        peers.push_back((c.local_rank + i) % c.num_nodes);
    }

    now = rdtsc();

    // Populate @send_queue with outgoing messages. Note: for the Hadoop policy,
    // we need to add outgoing messages when we receive the "pull" requests.
    if (op.policy != HADOOP) {
        for (int peer : peers) {
            op.tx_msgs[peer].last_ack_tsc = now;
            insert_sq(&op.send_queue, &op.tx_msgs[peer], opts.policy);
        }
    }

    QueueEstimator queue_estimator(opts.link_speed * 1000);

    // The TX thread loops until all msgs are sent; it paced itself based on
    // the TX link speed and put itself to sleep when possible.
    // In each iteration, the TX thread finds the first msg in @send_queue that
    // is allowed to transmit at least one more packet, sends out the packet,
    // and adjusts its position in the send queue accordingly.
    // In practice, the TX thread must also implement retransmission on timeout,
    // but maybe we can ignore that here? (two arguments: 1. buffer overflow is
    // rare (shouldn't happen in our experiment?); 2. timeout can be delegated
    // to Homa in a real impl.?).
    timestamp_create(search_start)
    rt::Mutex* lock_sq = opts.policy == HADOOP ? &op.send_mutex : nullptr;
    while (true) {
        if (lock_sq)
            lock_sq->Lock();
        if (list_empty(&op.send_queue)) {
            if (lock_sq) {
                lock_sq->Unlock();
                if (op.pull_reqs.load() < op.num_nodes - 1) {
                    tt_record("node-%d: TX thread waiting for more PULLs",
                            c.local_rank);
                    op.send_ready.DownAll();
                    continue;
                }
            }
            break;
        }

        // Find the next message to send.
        udp_out_msg* next_msg = nullptr;
        udp_out_msg* right_nb = nullptr;
        udp_out_msg* msg;
        list_for_each(&op.send_queue, msg, q_link) {
            uint32_t grant_limit = msg->send_wnd.load();
            if (msg->next_send_pkt < grant_limit) {
                next_msg = msg;
                right_nb = list_next(&op.send_queue, next_msg, q_link);
                if (opts.policy == HADOOP) {
                    // Rotate the list until @next_msg becomes the new head.
                    udp_out_msg* head;
                    while (true) {
                        head = list_pop(&op.send_queue, udp_out_msg, q_link);
                        if (head == next_msg) {
                            break;
                        } else {
                            list_add_tail(&op.send_queue, &head->q_link);
                        }
                    }
                }
                list_del_from(&op.send_queue, &msg->q_link);
                break;
            }
        }

        // Remove @next_msg if it's completed, or re-insert it into @send_queue
        // following the shuffle policy.
        size_t bytes_sent = 0;
        if (next_msg) {
            bytes_sent = op.max_payload * next_msg->next_send_pkt;
            next_msg->next_send_pkt++;
            if (next_msg->next_send_pkt >= next_msg->num_pkts) {
                tt_record("node-%d: removed message to node-%d from send_queue",
                        c.local_rank, next_msg->peer);
            } else {
                insert_sq(&op.send_queue, next_msg, opts.policy, right_nb);
            }
        }
        if (lock_sq)
            lock_sq->Unlock();

        timestamp_create(search_fin)
        percpu_metric_get(grpt_msg_cycles) +=
                timestamp_get(search_fin) - timestamp_get(search_start);

        if (next_msg == nullptr) {
            // If @send_ready is non-zero, clear the counter and retry;
            // otherwise, block until the RX thread gets more ACKs
            tt_record("node-%d: TX thread waiting for more ACKs", c.local_rank);
            op.send_ready.DownAll();
            tt_record("node-%d: TX thread woke up", c.local_rank);
            timestamp_update(search_start)
            idle_cyc += timestamp_get(search_start) - timestamp_get(search_fin);
            continue;
        }

        // Prepare the shuffle message header and payload.
        int peer = next_msg->peer;
        shfl_msg_buf& msg_buf = common_op.out_bufs[peer];
        size_t len = std::min(op.max_payload, msg_buf.len - bytes_sent);
        udp_shuffle_msg_hdr msg_hdr = {
            .op_id = (int16_t) common_op.id,
            .peer = (int16_t) c.local_rank,
            .tag = 0,
            .ack_no = 0,
            .grant_limit = 0,
            .seg_size = (uint16_t) len,
            .msg_size = (uint32_t) msg_buf.len,
            .start = (uint32_t) bytes_sent,
        };

        // Send the message as a vector.
        struct iovec iov[2];
        iov[0] = {.iov_base = &msg_hdr, .iov_len = sizeof(msg_hdr)};
        iov[1] = {.iov_base = msg_buf.addr + bytes_sent, .iov_len = len};
        netaddr laddr = op.local_addr;
        netaddr raddr = {c.server_list[peer], laddr.port};
        tt_record("node-%d: sending bytes %lu-%lu to node-%d",
                c.local_rank, bytes_sent, bytes_sent + len, peer);
        ssize_t ret = udp_sendv(iov, 2, laddr, raddr);
        if (ret != (ssize_t) (iov[0].iov_len + iov[1].iov_len)) {
            panic("WritevTo failed: unexpected return value %ld (expected %lu)",
                    ret, iov[0].iov_len + iov[1].iov_len);
        }
        percpu_metric_get(tx_data_pkts)++;

        now = rdtsc();
        timestamp_decl(send_fin)
        timestamp_get(send_fin) = now;
        percpu_metric_get(tx_data_cycles) +=
                timestamp_get(send_fin) - timestamp_get(search_fin);
        timestamp_get(search_start) = now;

        // Implement packet pacing to avoid pushing too many packets to the
        // network layer.
        uint32_t drain_us =
                queue_estimator.packetQueued(ret + UDP_IP_HDR_SIZE, now);
        // FIXME: well, simply trying to build up a larger TX buffer by
        // increasing drain_us is not going to eliminate uplink bubbles since
        // the number of buffered RX packets will also increase. As long as
        // the tx thread can't wake up in time, we will blow uplink bubbles.
        // e.g., what I usually observe from the time trace is that timer_finish_sleep
        // is already invoked a bit late because we are busy processing rx
        // packets and, to make things worse, timer_finish_sleep only puts the
        // tx thread at the end of the runqueue where there are usually several
        // rx threads queued ahead (they are created in the same softirq_fn call
        // as timer_softirq which invokes timer_finish_sleep.
        // As a result, I decided to use Yield as opposed to Sleep (combined w/
        // a relatively small drain_us) as a temporary hack.
        if (drain_us > 3) {
            // Sleep until the transmit queue is almost empty.
            uint32_t sleep_us = drain_us - 1;
#if 1
            while (rdtsc() < now + sleep_us * cycles_per_us) {
                rt::Yield();
            }
#else
            tt_record("about to sleep %u us", sleep_us);
            rt::Sleep(sleep_us);
#endif
            timestamp_update(search_start)
            idle_cyc += timestamp_get(search_start) - now;
        }
    }
    tt_record("node-%d: TX thread done, busy_cyc %u", c.local_rank,
            rdtsc() - timestamp_get(start) - idle_cyc);
}

/**
 * Handler function to free UDP shuffle object. This function will be invoked by
 * the caladan runtime right before @udp_spawner is freed.
 */
static void free_udp_shuffle_op(void* op)
{
    delete (udp_shuffle_op*) op;
    // FIXME: well, right now we can't guarantee that rx_thread will always
    // access the right/old common_op even if a packet arrives late; the correct
    // design might be to use a different shuffle_op for each run, and free the
    // old one here.
}

void
udp_shuffle_init(RunBenchOptions& opts, Cluster& c, shuffle_op& op)
{
    op.udp_shfl_obj = new udp_shuffle_op(&c, &op, &opts);
    netaddr laddr = {c.local_ip, opts.udp_port};
    int ret = udp_create_spawner(laddr, rx_thread, op.udp_shfl_obj,
            free_udp_shuffle_op, &udp_spawner);
    if (ret) {
        panic("udp_create_spawner failed (error code = %d)", ret);
    }
}

bool
udp_shuffle(RunBenchOptions& opts, Cluster& c, shuffle_op& op)
{
    tt_record("udp_shuffle: invoked");

    for (int cpu = 0; cpu < cpu_count; cpu++) {
        percpu_metric_get_remote(grpt_msg_cycles, cpu) = 0;
        percpu_metric_get_remote(tx_data_cycles, cpu) = 0;
        percpu_metric_get_remote(tx_ack_cycles, cpu) = 0;
        percpu_metric_get_remote(handle_ack_cycles, cpu) = 0;
        percpu_metric_get_remote(handle_data_cycles, cpu) = 0;
        percpu_metric_get_remote(rx_data_pkts, cpu) = 0;
        percpu_metric_get_remote(rx_ack_pkts, cpu) = 0;
        percpu_metric_get_remote(tx_data_pkts, cpu) = 0;
        percpu_metric_get_remote(tx_ack_pkts, cpu) = 0;
    }

    // Spin up a thread to copy the message destined to itself directly.
#if 0
    rt::Thread local_copy([&] {
        timestamp_create(start)
        tt_record("node-%d: copying local msg", c.local_rank);
        int self = c.local_rank;
        size_t len = op.out_bufs[self].len;
        op.in_bufs[self].addr = op.next_inmsg_addr.fetch_add(len);
        op.in_bufs[self].len = len;
        memcpy(op.in_bufs[self].addr, op.out_bufs[self].addr, len);
        timestamp_create(end)
        tt_record("local_copy_cycles %u",
                timestamp_get(end) - timestamp_get(start));
    });
#endif

    // Spin up a thread to do granting.
    udp_shuffle_op& udp_shfl_op = *(udp_shuffle_op*) op.udp_shfl_obj;
    rt::Thread grantor([&] { udp_grantor(c, op, udp_shfl_op, opts); });

    // The receive-side logic of shuffle will be triggered by ingress packet;
    // the rest of this method will handle the send-side logic.
    udp_tx_main(c, op, udp_shfl_op, opts);

    // And wait for the receive threads to finish.
    udp_shfl_op.shuffle_done.Down();
    udp_shfl_op.shuffle_done.Down();
//    local_copy.Join();
    tt_record("udp_shuffle: join RX thread");
    grantor.Join();

    // Print app-specific per-cpu stat counters to time trace.
    uint64_t m0, m1, m2;
    for (int cpu = 0; cpu < cpu_count; cpu++) {
        m0 = percpu_metric_get_remote(grpt_msg_cycles, cpu);
        m1 = percpu_metric_get_remote(tx_data_cycles, cpu);
        m2 = percpu_metric_get_remote(tx_data_pkts, cpu);
        if (m0 || m1 || m2)
            tt_record("cpu %02d, grpt_msg_cycles %u, tx_data_cycles %u, "
                      "tx_data_pkts %u", cpu, m0, m1, m2);

        m0 = percpu_metric_get_remote(tx_ack_cycles, cpu);
        m1 = percpu_metric_get_remote(handle_ack_cycles, cpu);
        m2 = percpu_metric_get_remote(handle_data_cycles, cpu);
        if (m0 || m1)
            tt_record("cpu %02d, tx_ack_cycles %u, handle_ack_cycles %u, "
                      "handle_data_cycles %u", cpu, m0, m1, m2);

        m0 = percpu_metric_get_remote(tx_ack_pkts, cpu);
        m1 = percpu_metric_get_remote(rx_data_pkts, cpu);
        m2 = percpu_metric_get_remote(rx_ack_pkts, cpu);
        if (m0 || m1 || m2)
            tt_record("cpu %02d, tx_ack_pkts %u, rx_data_pkts %u, "
                      "rx_ack_pkts %u", cpu, m0, m1, m2);
    }

    udp_destroy_spawner(udp_spawner);
    return true;
}