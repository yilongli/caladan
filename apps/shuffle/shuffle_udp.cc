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
    }

    inline void
    tt_record(uint64_t tsc, const char* format,
            uint64_t arg0 = 0, uint64_t arg1 = 0, uint64_t arg2 = 0,
            uint64_t arg3 = 0)
    {
        tt_record4_tsc(cpu_get_current(), tsc, format, arg0, arg1, arg2, arg3);
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

/// Network bandwidth available to our shuffle application, in Gbps.
static const size_t NETWORK_BANDWIDTH = 25;
//static const size_t NETWORK_BANDWIDTH = 10;

/// RTTbytes = bandwdithGbps * 1000 * RTT_us / 8
static const double RTT_BYTES = NETWORK_BANDWIDTH * 1e3 * 12.0 / 8;

/// Maximum size of the sliding window which controls the number of packets the
/// sender is allowed to send ahead of the last acknowledged packet.
static const size_t MAX_SEND_WND = size_t((RTT_BYTES + 1499) / 1500);

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
    /// the index of the last packet the sender is allowed to send.
    std::atomic<uint64_t> send_wnd;

//    /// Start index of the send window. This is the first packet in the message
//    /// which has not been acknowledged by the receiver.
//    std::atomic<size_t> send_wnd_start;
//
//    /// Index of the last packet that we are allowed to send; incremented based
//    /// on the ACK packets from the receiver.
//    std::atomic<size_t> send_limit;

    /// Index of the next packet to send.
    size_t next_send_pkt;

    /// Last time this message received an ACK, in rdtsc ticks.
    uint64_t last_ack_tsc;

    /// Used to chain this outbound message into the send queue.
    struct list_node q_link;

    explicit udp_out_msg(int peer, size_t num_pkts)
        : peer(peer)
        , num_pkts(num_pkts)
        , send_wnd()
//        , send_wnd_start()
//        , send_limit()
        , next_send_pkt()
        , last_ack_tsc(0)
        , q_link()
    {}

    /// Returns true if the number of remaining packets is less than @msg.
    bool operator<(const udp_out_msg& msg) const {
        return (num_pkts - next_send_pkt) < (msg.num_pkts - msg.next_send_pkt);
    }

    /// Returns true if the number of remaining packets is less than or equal to
    /// @msg.
    bool operator<=(const udp_out_msg& msg) const {
        return (num_pkts - next_send_pkt) <= (msg.num_pkts - msg.next_send_pkt);
    }
};

/**
 * Keep track of the progress of an inbound message.
 */
struct udp_in_msg {

    /// Minimum step to increment @grant_limit.
    static const size_t MIN_ACK_INC = 4;

    /// Network address of the message sender.
    netaddr remote_addr;

    /// Rank of the sender in the cluster.
    int peer;

    // TODO: revise the doc
    /// Protects @num_pkts, @recv_wnd_start, and @recv_wnd.
    rt::Mutex mutex;

    /// Number of packets in the message.
    size_t num_pkts;

    // TODO: keep it or not? not used in receiver-side GRPT, but could be useful in SRPT
    /// Indicate when to send back the next ACK (when @recv_wnd_start exceeds
    /// this value); used to reduce the number of ACK packets.
//    size_t next_ack_limit;

    /// Start index of the receive window. This is the first packet in the
    /// message which has not been received.
    size_t recv_wnd_start;

    /// Index of the last packet that has been granted.
    size_t grant_limit;

    /// Status of the packets in the receive window (the value indicates if
    /// the packet has been received). The entries in the window are organized
    /// as a ring, where the status of packet @recv_wnd_start is stored at index
    /// @recv_wnd_start % @MAX_SEND_WND.
    bool recv_wnd[MAX_SEND_WND];

    /// True if the corresponding shfl_msg_buf struct has been initialized.
    std::atomic_bool buf_inited;

    /// Used to chain this inbound message into udp_shuffle_op::grant_queue.
    struct list_node q_link;

    explicit udp_in_msg()
        : remote_addr()
        , peer(-1)
        , mutex()
        , num_pkts()
//        , next_ack_limit()
        , recv_wnd_start()
        , grant_limit()
        , recv_wnd()
        , buf_inited()
        , q_link()
    {}

    /// Return the number of packets that have not been granted.
    size_t ungranted_pkts() const {
        return num_pkts - grant_limit - 1;
    }

    /// Returns true if the number of ungranted packets is less than @msg.
    bool operator<(const udp_in_msg& msg) const {
        return ungranted_pkts() < msg.ungranted_pkts();
    }

    /// Returns true if the number of ungranted packets is less than or equal
    /// to @msg.
    bool operator<=(const udp_in_msg& msg) const {
        return ungranted_pkts() <= msg.ungranted_pkts();
    }
};

struct udp_shuffle_op {

    shuffle_op* common_op;

    const int local_rank;

    const int num_nodes;

    /// Local network address to use in this shuffle operation.
    const netaddr local_addr;

    // Keeps track of the status of each outbound message; shared between
    // the TX and RX threads.
    std::deque<udp_out_msg> tx_msgs;

    std::vector<udp_in_msg> rx_msgs;

    /// Number of outbound messages that have been fully acknowledged by the
    /// remote peers.
    std::atomic<int> acked_msgs;

    /// Number of inbound messages that have been fully received.
    std::atomic<int> completed_in_msgs;

    /// How many more probe packets to receive before we can start granting
    /// messages.
    std::atomic<int> more_probes;

    /// Protects @grant_queue.
    rt::Mutex grant_mutex;

    /// Number of grants that can be issued by the receiver (may drop below zero
    /// temporarily).
    int grants_avail;

    /// Priority queue which contains the inbound messages in @rx_msgs that are
    /// not yet completed; this is used to enforce the shuffle policy on the
    /// receiver side.
    struct list_head grant_queue;

    // FIXME: not a good idea to add another field that requires locking...
    /// Messages to be acked/granted. Always emtpy unless inside @rx_thread().
    /// Declared here to avoid dynamic memory allocation.
//    std::vector<udp_in_msg*> acks_to_send;

    /// Options used to customize the behavior of this shuffle operation.
    RunBenchOptions* options;

    /// Semaphore used to park the TX thread when all bytes in the sliding
    /// windows of the outbound messages have been transmitted (it can be
    /// woken up by the RX thread later when ACKs arrive).
    rt::Semaphore send_ready;

    rt::Semaphore shuffle_done;

    explicit udp_shuffle_op(Cluster* c, shuffle_op* common_op, uint16_t port)
        : common_op(common_op)
        , local_rank(c->local_rank)
        , num_nodes(c->num_nodes)
        , local_addr{c->local_ip, port}
        , tx_msgs()
        , rx_msgs(num_nodes)
        , acked_msgs(1)
        , completed_in_msgs(1)
        , more_probes(num_nodes - 1)
        , grant_mutex()
        // TODO: make this configurable
        , grants_avail(MAX_SEND_WND * 2)
        , grant_queue(LIST_HEAD_INIT(grant_queue))
//        , acks_to_send()
        , options()
        , send_ready(0)
        , shuffle_done(0)
    {
        size_t num_pkts;
        for (int i = 0; i < c->num_nodes; i++) {
            num_pkts = (i == c->local_rank) ? 0 :
                    (common_op->out_bufs[i].len + MAX_PAYLOAD - 1)/MAX_PAYLOAD;
            tx_msgs.emplace_back(i, num_pkts);
        }
    }
};

template <typename T>
static void insert_grpt(list_head *pq, T *start, T *new_msg)
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
    if (*new_msg <= *end) {
        list_add_tail(pq, &new_msg->q_link);
        return;
    }

    // Otherwise, locate the first entry smaller than @new_msg and insert
    // @new_msg right before it.
    while (true) {
        BUG_ON(!start);
        if (*start < *new_msg) {
            list_add_before(&start->q_link, &new_msg->q_link);
            return;
        }
        start = list_next(pq, start, q_link);
    }
}

//static void insert_grpt(list_head *pq, udp_out_msg *start, udp_out_msg *new_msg)
//{
//    // If @start is not specified, set @start to be the head of the queue.
//    if (!start) {
//        start = list_top(pq, struct udp_out_msg, q_link);
//        if (!start) {
//            list_add(pq, &new_msg->q_link);
//            return;
//        }
//    }
//
//    // Optimization: if @new_msg is smaller than the last entry, just
//    // insert it at the tail of the queue.
//    udp_out_msg* end = list_tail(pq, struct udp_out_msg, q_link);
//    if (*new_msg <= *end) {
//        list_add_tail(pq, &new_msg->q_link);
//        return;
//    }
//
//    // Otherwise, locate the first entry smaller than @new_msg and insert
//    // @new_msg right before it.
//    while (true) {
//        BUG_ON(!start);
//        if (*start < *new_msg) {
//            list_add_before(&start->q_link, &new_msg->q_link);
//            return;
//        }
//        start = list_next(pq, start, q_link);
//    }
//}

static void insert_srpt(list_head *pq, udp_out_msg *new_msg)
{
    // Locate the first entry larger than @new_msg and insert @new_msg right
    // before it.
    udp_out_msg *m = list_top(pq, struct udp_out_msg, q_link);
    while (m) {
        if (*new_msg < *m) {
            list_add_before(&m->q_link, &new_msg->q_link);
            return;
        }
        m = list_next(pq, m, q_link);
    }
    list_add_tail(pq, &new_msg->q_link);
}

static void insert_rr(list_head *pq, udp_out_msg *new_msg, int rr_k)
{
    // Locate the (rr_k - 1)-th entry in the queue and insert @new_msg after it;
    // if the queue is not long enough, insert to the tail.
    udp_out_msg *m, *prev;

    m = list_top(pq, struct udp_out_msg, q_link);
    BUG_ON(rr_k < 1);
    if (!m || (rr_k == 1)) {
        list_add(pq, &new_msg->q_link);
        return;
    }

    for (int i = 0; i < rr_k - 2; i++) {
        prev = m;
        m = list_next(pq, m, q_link);
        if (!m) {
            list_add_after(&prev->q_link, &new_msg->q_link);
            return;
        }
    }
    list_add_after(&m->q_link, &new_msg->q_link);
}

/// Insert outgoing message @new_msg to @send_queue based on the shuffle policy.
static void insert_pq(list_head *send_queue, udp_out_msg *new_msg,
        ShufflePolicy policy, int max_out_msgs, udp_out_msg *start = nullptr)
{
    switch (policy) {
        case ShufflePolicy::HADOOP:
            insert_rr(send_queue, new_msg, max_out_msgs);
            break;
        case ShufflePolicy::LOCKSTEP:
            list_add(send_queue, &new_msg->q_link);
            break;
        case ShufflePolicy::LRPT:
            insert_grpt(send_queue, start, new_msg);
            break;
        case ShufflePolicy::SRPT:
            insert_srpt(send_queue, new_msg);
            break;
        default:
            panic("unexpected policy value: %d", policy);
    }
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

    struct ack_info {
        uint16_t ack_no;
        uint16_t grant_limit;
        udp_in_msg* msg;
    };
    // todo: avoid dynamic memory alloc?
    std::vector<ack_info> acks_to_send;

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
        tt_record("node-%d: dropped obsolete packet from op %d, is_ack %u, "
                  "ack_no %u", msg_hdr.op_id, msg_hdr.is_ack, msg_hdr.ack_no);
        goto done;
    }

    if (msg_hdr.is_ack) {
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
            uint64_t old_val = tx_msg.send_wnd.load();
            uint64_t new_val =
                    (uint64_t) msg_hdr.ack_no << 32u | msg_hdr.grant_limit;
            while (new_val > old_val) {
                if (tx_msg.send_wnd.compare_exchange_strong(old_val, new_val)) {
                    op->send_ready.Up();
                    break;
                }
            }
        }
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
        size_t pkt_idx = msg_hdr.start / MAX_PAYLOAD;
        udp_in_msg& rx_msg = op->rx_msgs[peer];
        char* buf;
        if (unlikely(!rx_msg.buf_inited.load(std::memory_order_acquire))) {
            percpu_metric_scoped_cs(handle_data_cycles, rx_msg.mutex, start)
            if (!rx_msg.buf_inited.load(std::memory_order_acquire)) {
                buf = common_op->next_inmsg_addr.fetch_add(msg_hdr.msg_size,
                        std::memory_order_relaxed);
                common_op->in_bufs[peer].addr = buf;
                common_op->in_bufs[peer].len = msg_hdr.msg_size;
                op->more_probes--;
                rx_msg.remote_addr = d->raddr;
                rx_msg.peer = peer;
                rx_msg.num_pkts =
                        (msg_hdr.msg_size + MAX_PAYLOAD - 1) / MAX_PAYLOAD;
                rx_msg.buf_inited.store(true, std::memory_order_release);

                // FIXME: don't hardcode the policy; generalize insert_pq w/ template
                rt::ScopedLock<rt::Mutex> _(&op->grant_mutex);
                insert_grpt(&op->grant_queue, (udp_in_msg*) nullptr, &rx_msg);
            }
        }
        buf = common_op->in_bufs[peer].addr;

        // Read the shuffle payload.
        memcpy(buf + msg_hdr.start, mbuf + sizeof(msg_hdr), msg_hdr.seg_size);

        // Attempt to advance the sliding window.
        bool rx_msg_complete;
        {
            percpu_metric_scoped_cs(handle_data_cycles, rx_msg.mutex, start)
            rx_msg.recv_wnd[pkt_idx % MAX_SEND_WND] = true;
            while (true) {
                size_t idx = rx_msg.recv_wnd_start % MAX_SEND_WND;
                if (!rx_msg.recv_wnd[idx]) {
                    break;
                }
                rx_msg.recv_wnd[idx] = false;
                rx_msg.recv_wnd_start++;
            }
            rx_msg_complete = (rx_msg.recv_wnd_start >= rx_msg.num_pkts);

            // The following code must acquire the grant mutex before accessing
            // the grant queue.
            rt::ScopedLock<rt::Mutex> _(&op->grant_mutex);

            if (rx_msg_complete) {
                acks_to_send.emplace_back(???, ???, &rx_msg);
            }

            // TODO: make unscheduled pkts configurable?
            // Refill the grants if the packet was granted by us earlier.
            if (pkt_idx >= 1) {
                op->grants_avail++;
            }

            udp_in_msg* top_msg;
            while (!list_empty(&op->grant_queue) && (op->grants_avail > 0)) {
                top_msg = list_top(&op->grant_queue, udp_in_msg, q_link);
                list_del_from(&op->grant_queue, &top_msg->q_link);
                size_t grants = std::min(top_msg->ungranted_pkts(),
                        udp_in_msg::MIN_ACK_INC);
                top_msg->grant_limit += grants;
                op->grants_avail -= grants;
                // FIXME: insert_pq uses operator< internally, but acquiring individual udp_in_msg::mutex can be a perf. disaster.
                // TODO: use insert_pq instead
                insert_grpt(&op->grant_queue, (udp_in_msg*) nullptr, top_msg);

                op->acks_to_send.push_back(top_msg);
            }
        }

        if (rx_msg_complete) {
            tt_record("node-%d: received message from node-%d",
                    op->local_rank, peer);
            if (op->completed_in_msgs.fetch_add(1) + 1 == op->num_nodes) {
                // All inbound messages are received.
                tt_record("node-%d: all in msgs received", op->local_rank);
                op->shuffle_done.Up();
            }
        }

        if (!acks_to_send.empty()) {
            // Send back ACKs.
            timestamp_update(send_ack)
            udp_shuffle_msg_hdr ack_hdr = {
                .op_id = (int16_t) common_op->id,
                .peer = (int16_t) op->local_rank,
                .is_ack = 1,
//                .ack_no = 0, .seg_size = 0, .msg_size = 0, .start = 0,
            };
            for (auto info : acks_to_send) {
                ack_hdr.ack_no = info.ack_no;
                ack_hdr.grant_limit = info.grant_limit;
                tt_record("node-%d: sending ACK %u to node-%d", op->local_rank,
                        ack_hdr.ack_no, info.msg->peer);
                udp_send(&ack_hdr, sizeof(ack_hdr), op->local_addr,
                        info.msg->remote_addr);
            }
            percpu_metric_get(tx_ack_pkts) += acks_to_send.size();
        }
    }

  done:
    udp_spawn_data_release(d->release_data);
    timestamp_create(end)
    if (msg_hdr.is_ack) {
        percpu_metric_get(rx_ack_pkts)++;
        percpu_metric_get(handle_ack_cycles) +=
                timestamp_get(end) - timestamp_get(start);
    } else {
        if (num_acks == 0) {
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
udp_tx_main(Cluster& c, shuffle_op& common_op, udp_shuffle_op& op)
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
    if (op.options->policy == HADOOP) {
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(peers.begin(), peers.end(), g);
    }

    // @send_queue: main data structure used to implement the shuffle policy.
    struct list_head send_queue = LIST_HEAD_INIT(send_queue);
    now = rdtsc();

    // Populate @send_queue with outgoing messages.
    RunBenchOptions* opts = op.options;
    for (int peer : peers) {
        insert_pq(&send_queue, &op.tx_msgs[peer], opts->policy,
                opts->max_out_msgs);
    }

    // FIXME: make the link speed configurable via cmd line?
    QueueEstimator queue_estimator(NETWORK_BANDWIDTH * 1000);

    // FIXME: explain how this is used to implement the probe phase
    udp_out_msg* probe_msg = list_top(&send_queue, udp_out_msg, q_link);

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
    while (!list_empty(&send_queue)) {
        // Find the next message to send.
        udp_out_msg* next_msg = nullptr;
        udp_out_msg* right_nb = nullptr;

        bool probe_phase = (probe_msg != nullptr);
        if (probe_phase) {
            // Probe phase: send one packet from each of the outbound messages
            next_msg = probe_msg;
            probe_msg = list_next(&send_queue, probe_msg, q_link);
            if (next_msg->num_pkts <= 1) {
                list_del_from(&send_queue, &next_msg->q_link);
            }
        } else {
            // Normal phase: send from the message with the highest priority
            udp_out_msg* msg;
            size_t msg_idx = 0;
            list_for_each(&send_queue, msg, q_link) {
                // Only consider the top-N outbound messages in the send queue.
                if (msg_idx >= opts->max_out_msgs) {
                    break;
                }
                msg_idx++;

                uint32_t grant_limit = msg->send_wnd.load();
                if (msg->next_send_pkt <= grant_limit) {
                    next_msg = msg;
                    right_nb = list_next(&send_queue, next_msg, q_link);
                    list_del_from(&send_queue, &msg->q_link);
                    break;
                }
            }
        }


        // In the normal phase, @next_msg is removed in the loop above;
        // re-insert it into @send_queue if it's not completed yet.
        size_t bytes_sent = 0;
        if (next_msg) {
            bytes_sent = MAX_PAYLOAD * next_msg->next_send_pkt;
            next_msg->next_send_pkt++;
            if (next_msg->next_send_pkt >= next_msg->num_pkts) {
                tt_record("node-%d: removed message to node-%d from send_queue",
                        c.local_rank, next_msg->peer);
            } else if (!probe_phase) {
                insert_pq(&send_queue, next_msg, opts->policy,
                        opts->max_out_msgs, right_nb);
            }
        }

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
        size_t len = std::min(MAX_PAYLOAD, msg_buf.len - bytes_sent);
        udp_shuffle_msg_hdr msg_hdr = {
            .op_id = (int16_t) common_op.id,
            .peer = (int16_t) c.local_rank,
            .is_ack = 0,
            .ack_no = 0,
            .seg_size = (uint16_t) len,
            .msg_size = (uint32_t) msg_buf.len,
            .start = (uint32_t) bytes_sent,
        };

        // Send the message as a vector.
        struct iovec iov[2];
        iov[0] = {.iov_base = &msg_hdr, .iov_len = sizeof(msg_hdr)};
        iov[1] = {.iov_base = msg_buf.addr + bytes_sent, .iov_len = len};
        netaddr laddr = op.local_addr;
        netaddr raddr = {c.server_list[peer], op.local_addr.port};
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
        if (drain_us > 1) {
            // Sleep until the transmit queue is almost empty.
            tt_record("about to sleep %u us", drain_us - 1);
            rt::Sleep(drain_us - 1);
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
    op.udp_shfl_obj = new udp_shuffle_op(&c, &op, opts.udp_port);
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

    // The receive-side logic of shuffle will be triggered by ingress packet;
    // the rest of this method will handle the send-side logic.
    udp_shuffle_op& udp_shfl_op = *(udp_shuffle_op*) op.udp_shfl_obj;
    udp_shfl_op.options = &opts;
    udp_tx_main(c, op, udp_shfl_op);

    // And wait for the receive threads to finish.
    udp_shfl_op.shuffle_done.Down();
    udp_shfl_op.shuffle_done.Down();
//    local_copy.Join();
    tt_record("udp_shuffle: join RX thread");

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