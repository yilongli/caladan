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

/// Size of the sliding window which controls the number of packets the sender
/// is allowed to send ahead of the last acknowledged packet.
static const size_t SEND_WND_SIZE = size_t((RTT_BYTES + 1499) / 1500);

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

    /// Protects @send_wnd_start. Declared as an unique_ptr so that udp_out_msg
    /// can be put inside a vector (rt::Mutex is not copy/move-constructible).
    std::unique_ptr<rt::Mutex> send_wnd_mutex;

    /// Start index of the send window. This is the first packet in the message
    /// which has not been acknowledged by the receiver.
    size_t send_wnd_start;

    /// Index of the next packet to send.
    size_t next_send_pkt;

    /// Last time this message received an ACK, in rdtsc ticks.
    uint64_t last_ack_tsc;

    /// Used to chain this outbound message into the send queue.
    struct list_node sq_link;

    explicit udp_out_msg(int peer, size_t num_pkts)
        : peer(peer)
        , num_pkts(num_pkts)
        , send_wnd_mutex(std::make_unique<rt::Mutex>())
        , send_wnd_start()
        , next_send_pkt()
        , last_ack_tsc()
        , sq_link()
    {}
};

/**
 * Keep track of the progress of an inbound message.
 */
struct udp_in_msg {

    /// Minimum step to increment @next_ack_limit.
    static const size_t MIN_ACK_INC = SEND_WND_SIZE / 4;

    /// Protects @num_pkts, @recv_wnd_start, and @recv_wnd.
    rt::Mutex mutex;

    /// Number of packets in the message.
    size_t num_pkts;

    /// Indicate when to send back the next ACK (when @recv_wnd_start exceeds
    /// this value); used to reduce the number of ACK packets.
    size_t next_ack_limit;

    /// Start index of the receive window. This is the first packet in the
    /// message which has not been received.
    size_t recv_wnd_start;

    /// Status of the packets in the receive window (the value indicates if
    /// the packet has been received). The entries in the window are organized
    /// as a ring, where the status of packet @recv_wnd_start is stored at index
    /// @recv_wnd_start mod @SEND_WND_SIZE.
    bool recv_wnd[SEND_WND_SIZE];

    /// True if the corresponding shfl_msg_buf struct has been initialized.
    std::atomic_bool buf_inited;

    explicit udp_in_msg()
        : mutex()
        , num_pkts()
        , next_ack_limit(MIN_ACK_INC)
        , recv_wnd_start()
        , recv_wnd()
        , buf_inited()
    {}
};

struct udp_shuffle_op {

    shuffle_op* common_op;

    const int local_rank;

    const int num_nodes;

    /// UDP port number used by @udp_spawner.
    const uint16_t port;

    // Keeps track of the status of each outbound message; shared between
    // the TX and RX threads.
    std::vector<udp_out_msg> tx_msgs;

    std::vector<udp_in_msg> rx_msgs;

    /// Number of outbound messages that have been fully acknowledged by the
    /// remote peers.
    std::atomic<int> acked_msgs;

    /// Number of inbound messages that have been fully received.
    std::atomic<int> completed_in_msgs;

    /// Semaphore used to park the TX thread when all bytes in the sliding
    /// windows of the outbound messages have been transmitted (it can be
    /// woken up by the RX thread later when ACKs arrive).
    rt::Semaphore send_ready;

    rt::Semaphore shuffle_done;

    explicit udp_shuffle_op(Cluster* c, shuffle_op* common_op, uint16_t port)
        : common_op(common_op)
        , local_rank(c->local_rank)
        , num_nodes(c->num_nodes)
        , port(port)
        , tx_msgs()
        , rx_msgs(num_nodes)
        , acked_msgs(1)
        , completed_in_msgs(1)
        , send_ready(0)
        , shuffle_done(0)
    {
        tx_msgs.reserve(c->num_nodes);
        size_t num_pkts;
        for (int i = 0; i < c->num_nodes; i++) {
            num_pkts = (i == c->local_rank) ? 0 :
                    (common_op->out_bufs[i].len + MAX_PAYLOAD - 1)/MAX_PAYLOAD;
            tx_msgs.emplace_back(i, num_pkts);
        }
    }
};

/**
 * Main function of the RX thread that is responsible for receiving packets,
 * assembling inbound messages, and sending back ACKs.
 */
static void rx_thread(struct udp_spawn_data *d)
{
    auto* op = (udp_shuffle_op*) d->app_state;
    auto* common_op = op->common_op;
    bool send_ack = false;
    uint64_t start_tsc = rdtsc();

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
        tt_record(start_tsc, "node-%d: received ACK %u from node-%d",
                op->local_rank, msg_hdr.ack_no, peer);
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
            percpu_metric_scoped_cs(handle_ack_cycles, *tx_msg.send_wnd_mutex,
                    start)
            if (msg_hdr.ack_no > tx_msg.send_wnd_start) {
                tx_msg.send_wnd_start = msg_hdr.ack_no;
                op->send_ready.Up();
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
                rx_msg.num_pkts =
                        (msg_hdr.msg_size + MAX_PAYLOAD - 1) / MAX_PAYLOAD;
                rx_msg.buf_inited.store(true, std::memory_order_release);
            }
        }
        buf = common_op->in_bufs[peer].addr;

        // Read the shuffle payload.
        memcpy(buf + msg_hdr.start, mbuf + sizeof(msg_hdr), msg_hdr.seg_size);

        // Attempt to advance the sliding window.
        bool rx_msg_complete;
        {
            percpu_metric_scoped_cs(handle_data_cycles, rx_msg.mutex, start)
            rx_msg.recv_wnd[pkt_idx % SEND_WND_SIZE] = true;
            while (true) {
                size_t idx = rx_msg.recv_wnd_start % SEND_WND_SIZE;
                if (!rx_msg.recv_wnd[idx]) {
                    break;
                }
                rx_msg.recv_wnd[idx] = false;
                rx_msg.recv_wnd_start++;
            }
            rx_msg_complete = (rx_msg.recv_wnd_start >= rx_msg.num_pkts);
            if (rx_msg.recv_wnd_start > rx_msg.next_ack_limit) {
                send_ack = true;
                rx_msg.next_ack_limit += udp_in_msg::MIN_ACK_INC;
            }
        }

        if (rx_msg_complete) {
            tt_record("node-%d: received message from node-%d",
                    op->local_rank, peer);
            send_ack = true;
            if (op->completed_in_msgs.fetch_add(1) + 1 == op->num_nodes) {
                // All inbound messages are received.
                tt_record("node-%d: all in msgs received", op->local_rank);
                op->shuffle_done.Up();
            }
        }

        if (send_ack) {
            // Send back an ACK.
            timestamp_update(send_ack)
            udp_shuffle_msg_hdr ack_hdr = {
                .op_id = msg_hdr.op_id,
                .peer = (int16_t) op->local_rank,
                .is_ack = 1,
                .ack_no = (uint16_t) rx_msg.recv_wnd_start,
//                .seg_size = 0, .msg_size = 0, .start = 0,
            };
            tt_record("node-%d: sending ACK %u to node-%d", op->local_rank,
                    ack_hdr.ack_no, peer);
            udp_respond(&ack_hdr, sizeof(ack_hdr), d);
            percpu_metric_get(tx_ack_pkts)++;
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
        if (!send_ack) {
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

    // @send_queue is the main data structure used to implement the LRPT policy.
    struct list_head send_queue = LIST_HEAD_INIT(send_queue);
    now = rdtsc();
    for (int i = 1; i < c.num_nodes; i++) {
        int peer = (c.local_rank + i) % c.num_nodes;
        op.tx_msgs[peer].last_ack_tsc = now;
        list_add(&send_queue, &op.tx_msgs[peer].sq_link);
    }

    // FIXME: how to set the link speed properly???
    QueueEstimator queue_estimator(NETWORK_BANDWIDTH * 1000);

    // The TX thread loops until all msgs are sent; it paced itself based on
    // the TX link speed and put itself to sleep when possible.
    // In every iteration, the TX thread finds the longest msg that hasn't
    // filled up its send window and transmits one more packet, adjusts its pos
    // in the LRPT queue accordingly.
    // In practice, the TX thread must also implement retransmission on timeout,
    // but maybe we can ignore that here? (two arguments: 1. buffer overflow is
    // rare (shouldn't happen in our experiment?); 2. timeout can be delegated
    // to Homa in a real impl.?).
    timestamp_create(search_start)
    while (!list_empty(&send_queue)) {
        // FIXME: better impl. to avoid scanning the entire send queue?
        // Search for the message which has the maximum number of packets left
        // and not reached its send limit.
        udp_out_msg* next_msg = nullptr;
        size_t max_pkts_left = 0;
        udp_out_msg* msg;
        list_for_each(&send_queue, msg, sq_link) {
            // Acquiring a mutex could move the current uthread to another cpu,
            // in order to keep accurate per-cpu cycles, exclude the time inside
            // rt::Mutex::Lock().
            percpu_metric_scoped_cs(grpt_msg_cycles, *msg->send_wnd_mutex,
                    search_start)
            size_t send_wnd_start = msg->send_wnd_start;
            assert(msg->next_send_pkt >= send_wnd_start);
            size_t pkts_left = msg->num_pkts - msg->next_send_pkt;
            size_t unacked_pkts = msg->next_send_pkt - send_wnd_start;
            if ((unacked_pkts < SEND_WND_SIZE) && (pkts_left > max_pkts_left)) {
                next_msg = msg;
                max_pkts_left = pkts_left;
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
        size_t bytes_sent = MAX_PAYLOAD * next_msg->next_send_pkt;
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
        netaddr laddr = {c.local_ip, op.port};
        netaddr raddr = {c.server_list[peer], op.port};
        tt_record("node-%d: sending bytes %lu-%lu to node-%d",
                c.local_rank, bytes_sent, bytes_sent + len, peer);
        ssize_t ret = udp_sendv(iov, 2, laddr, raddr);
        if (ret != (ssize_t) (iov[0].iov_len + iov[1].iov_len)) {
            panic("WritevTo failed: unexpected return value %ld (expected %lu)",
                    ret, iov[0].iov_len + iov[1].iov_len);
        }
        percpu_metric_get(tx_data_pkts)++;

        next_msg->next_send_pkt++;
        if (next_msg->next_send_pkt >= next_msg->num_pkts) {
            list_del_from(&send_queue, &next_msg->sq_link);
            tt_record("node-%d: removed message to node-%d from send_queue",
                    c.local_rank, peer);
        }

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

    if (opts.policy != ShufflePolicy::LRPT) {
        log_err("only LRPT policy is implemented");
        return false;
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