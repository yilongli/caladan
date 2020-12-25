#include "shuffle_udp.h"

extern "C" {
#include <base/log.h>
#include <net/ip.h>
#include <runtime/timetrace.h>
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
        tt_record4_np(format, arg0, arg1, arg2, arg3);
    }
}

/// Maximum number of bytes that can fit in a UDP-based shuffle message.
static const size_t MAX_PAYLOAD = UDP_MAX_PAYLOAD - sizeof(udp_shuffle_msg_hdr);

/// Size of the UDP and IP headers, in bytes.
static size_t UDP_IP_HDR_SIZE = sizeof(udp_hdr) + sizeof(ip_hdr);

/// Network bandwidth available to our shuffle application, in Gbps.
static const size_t NETWORK_BANDWIDTH = 25;
//static const size_t NETWORK_BANDWIDTH = 10;

/// RTTbytes = bandwdithGbps * 1000 * RTT_us / 8
static const double RTT_BYTES = NETWORK_BANDWIDTH * 1e3 * 7.0 / 8;

/// Size of the sliding window which controls the number of packets the sender
/// is allowed to send ahead of the last acknowledged packet.
static const size_t SEND_WND_SIZE = size_t((RTT_BYTES + 1499) / 1500);

/**
 * Keep track of the progress of an outbound message.
 */
struct udp_out_msg {

    /// Rank of the message receiver.
    const int peer;

    /// Total number of packets in this message.
    const size_t num_pkts;

    // FIXME: its declared as unique_ptr to make vector happy
    /// Protects @send_wnd_start.
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

    /// Number of packets in the message.
    size_t num_pkts;

    /// Start index of the receive window. This is the first packet in the
    /// message which has not been received.
    size_t recv_wnd_start;

    /// Status of the packets in the receive window (the value indicates if
    /// the packet has been received). The entries in the window are organized
    /// as a ring, where the status of packet @recv_wnd_start is stored at index
    /// @recv_wnd_start mod @SEND_WND_SIZE.
    bool recv_wnd[SEND_WND_SIZE];

    explicit udp_in_msg()
        : num_pkts()
        , recv_wnd_start()
        , recv_wnd()
    {}
};

/**
 * Main function of the RX thread that is responsible for receiving packets,
 * assembling inbound messages, and sending back ACKs.
 *
 * \param c
 *      Cluster object.
 * \param op
 *      Shuffle object that keeps track of the progress.
 * \param udp_port
 *      Port number of the UDP socket used to receive packets.
 * \param tx_msgs
 *      List of outbound messages indexed by the rank of the receiver.
 */
void
udp_rx_main(Cluster& c, shuffle_op& op, uint16_t udp_port,
        std::vector<udp_out_msg>& tx_msgs)
{
    uint64_t total_cyc, idle_cyc;
    uint64_t now;

    // FIXME: what if we want to use multiple UDP sockets?
    rt::UdpConn* udp_sock = c.udp_socks[udp_port].get();
    if (udp_sock == nullptr) {
        panic("no UDP socket available at port %u", udp_port);
    }

    //
    std::vector<udp_in_msg> rx_msgs;
    rx_msgs.reserve(c.num_nodes);
    for (int i = 0; i < c.num_nodes; i++) {
        rx_msgs.emplace_back();
    }

    total_cyc = rdtsc();
    idle_cyc = 0;
    netaddr raddr{};
    char mbuf[2048];
    int completed_in_msgs = 1;
    int acked_msgs = 1;
    while ((completed_in_msgs < c.num_nodes) || (acked_msgs < c.num_nodes)) {
        // Read the next message header.
        // TODO: how to handle a wave of DATA packets that come in the same batch at the driver level? (ideally we want to send out just a single ACK)
        now = rdtsc();
        ssize_t mbuf_size = udp_sock->ReadFrom(mbuf, ARRAY_SIZE(mbuf), &raddr);
        // FIXME: this is not accurate; because readFrom actually perform quite a bit work!!!
        idle_cyc += rdtsc() - now;
        auto& msg_hdr = *reinterpret_cast<udp_shuffle_msg_hdr*>(mbuf);
        if (mbuf_size < (ssize_t) sizeof(msg_hdr)) {
            panic("unknown mbuf size %ld (expected at least %lu bytes)",
                    mbuf_size, sizeof(msg_hdr));
        }

        int peer = msg_hdr.peer;
        if (msg_hdr.op_id != op.id) {
            tt_record("node-%d: dropped obsolete packet from op %d, ack_no %u",
                    msg_hdr.op_id, msg_hdr.ack_no);
            continue;
        }
        if (msg_hdr.ack_no > 0) {
            // ACK message.
            tt_record("node-%d: received ACK %u from node-%d", c.local_rank,
                    msg_hdr.ack_no, peer);
            auto& tx_msg = tx_msgs[peer];
            tx_msg.last_ack_tsc = rdtsc();
            if (msg_hdr.ack_no >= tx_msg.num_pkts) {
                acked_msgs++;
            }
            rt::ScopedLock<rt::Mutex> _(tx_msg.send_wnd_mutex.get());
            if (msg_hdr.ack_no > tx_msg.send_wnd_start) {
                tx_msg.send_wnd_start = msg_hdr.ack_no;
                op.udp_send_ready->Up();
            }
        } else {
            // Normal data segment.
            tt_record("node-%d: receiving bytes %lu-%lu from node-%d",
                    c.local_rank, msg_hdr.start,
                    msg_hdr.start + msg_hdr.seg_size, peer);

            // Initialize the memory buffer to hold the inbound message, if this
            // is the first packet of this message.
            char* buf = op.in_bufs[peer].addr;
            if (!buf) {
                buf = op.next_inmsg_addr.fetch_add(msg_hdr.msg_size,
                        std::memory_order_relaxed);
                op.in_bufs[peer].addr = buf;
                op.in_bufs[peer].len = msg_hdr.msg_size;
                rx_msgs[peer].num_pkts =
                        (msg_hdr.msg_size + MAX_PAYLOAD - 1) / MAX_PAYLOAD;
            }

            // Read the shuffle payload.
            // FIXME: this is one extra memcpy... how to avoid it?
            memcpy(buf + msg_hdr.start, mbuf + sizeof(msg_hdr),
                    msg_hdr.seg_size);
            if (mbuf_size - sizeof(msg_hdr) != msg_hdr.seg_size) {
                panic("unexpected payload size %ld (expected %u)",
                        mbuf_size - sizeof(msg_hdr), msg_hdr.seg_size);
            }

            // Attempt to advance the sliding window.
            size_t pkt_idx = msg_hdr.start / MAX_PAYLOAD;
            udp_in_msg& rx_msg = rx_msgs[peer];
            rx_msg.recv_wnd[pkt_idx % SEND_WND_SIZE] = true;
            while (true) {
                size_t idx = rx_msg.recv_wnd_start % SEND_WND_SIZE;
                if (!rx_msg.recv_wnd[idx]) {
                    break;
                }
                rx_msg.recv_wnd[idx] = false;
                rx_msg.recv_wnd_start++;
            }
            if (rx_msg.recv_wnd_start >= rx_msg.num_pkts) {
                completed_in_msgs++;
                tt_record("node-%d: received message from node-%d",
                        c.local_rank, peer);
            }

            // Send back an ACK.
            udp_shuffle_msg_hdr ack_hdr;
            ack_hdr.op_id = op.id;
            ack_hdr.ack_no = rx_msg.recv_wnd_start;
            ack_hdr.peer = c.local_rank;
            tt_record("node-%d: sending ACK %u to node-%d", c.local_rank,
                    ack_hdr.ack_no, peer);
            udp_sock->WriteTo(&ack_hdr, sizeof(ack_hdr), &raddr);
        }
    }

    total_cyc = rdtsc() - total_cyc;
    uint64_t busy_cyc = total_cyc - idle_cyc;
    tt_record("node-%d: RX thread load factor 0.%u, busy %u, idle %u",
            c.local_rank, busy_cyc * 100 / total_cyc, busy_cyc, idle_cyc);
}

/**
 * Main function of the TX thread that is responsible for scheduling outbound
 * messages, enforcing flow control (using a sliding window), and retransmitting
 * packets on timeouts.
 *
 * \param c
 *      Cluster object.
 * \param op
 *      Shuffle object that keeps track of the progress.
 * \param udp_port
 *      Port number of the UDP socket used to send packets.
 * \param tx_msgs
 *      List of outbound messages indexed by the rank of the receiver.
 */
void
udp_tx_main(Cluster& c, shuffle_op& op, uint16_t udp_port,
        std::vector<udp_out_msg>& tx_msgs)
{
    uint64_t total_cyc, idle_cyc;
    uint64_t now;

    // @send_queue is the main data structure used to implement the LRPT policy.
    struct list_head send_queue = LIST_HEAD_INIT(send_queue);
    for (int i = 1; i < c.num_nodes; i++) {
        int peer = (c.local_rank + i) % c.num_nodes;
        tx_msgs[peer].last_ack_tsc = now;
        list_add(&send_queue, &tx_msgs[peer].sq_link);
    }

    // FIXME: what if we want to use multiple UDP sockets?
    rt::UdpConn* udp_sock = c.udp_socks[udp_port].get();
    if (udp_sock == nullptr) {
        panic("no UDP socket available at port %u", udp_port);
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
    total_cyc = rdtsc();
    idle_cyc = 0;
    while (!list_empty(&send_queue)) {
        // FIXME: better impl. to avoid scanning the entire send queue?

        // Search for the message which has the maximum number of packets left
        // and not reached its send limit.
        udp_out_msg* next_msg = nullptr;
        size_t max_pkts_left = 0;
        udp_out_msg* msg;
        list_for_each(&send_queue, msg, sq_link) {
            rt::ScopedLock<rt::Mutex> _(msg->send_wnd_mutex.get());
            size_t send_wnd_start = msg->send_wnd_start;
            assert(msg->next_send_pkt >= send_wnd_start);
            size_t pkts_left = msg->num_pkts - msg->next_send_pkt;
            size_t unacked_pkts = msg->next_send_pkt - send_wnd_start;
            if ((unacked_pkts < SEND_WND_SIZE) && (pkts_left > max_pkts_left)) {
                next_msg = msg;
                max_pkts_left = pkts_left;
            }
        }
        if (next_msg == nullptr) {
            /* block until the RX thread gets more ACKs */
            tt_record("node-%d: TX thread waiting for more ACKs", c.local_rank);
            now = rdtsc();
            op.udp_send_ready->DownAll();
            idle_cyc += rdtsc() - now;
            continue;
        }

        // Prepare the shuffle message header and payload.
        int peer = next_msg->peer;
        shfl_msg_buf& msg_buf = op.out_bufs[peer];
        size_t bytes_sent = MAX_PAYLOAD * next_msg->next_send_pkt;
        size_t len = std::min(MAX_PAYLOAD, msg_buf.len - bytes_sent);
        udp_shuffle_msg_hdr msg_hdr = {
            .ack_no = 0,
            .seg_size = (uint16_t) len,
            .msg_size = (uint32_t) msg_buf.len,
            .start = (uint32_t) bytes_sent,
            .peer = (int16_t) c.local_rank,
            .op_id = (int16_t) op.id
        };

        // Send the message as a vector.
        struct iovec iov[2];
        iov[0] = {.iov_base = &msg_hdr, .iov_len = sizeof(msg_hdr)};
        iov[1] = {.iov_base = msg_buf.addr + bytes_sent, .iov_len = len};
        netaddr raddr = {c.server_list[peer], udp_port};
        tt_record("node-%d: sending bytes %lu-%lu to node-%d",
                c.local_rank, bytes_sent, bytes_sent + len, peer);
        ssize_t ret = udp_sock->WritevTo(iov, 2, &raddr);
        if (ret != (ssize_t) (iov[0].iov_len + iov[1].iov_len)) {
            panic("WritevTo failed: unexpected return value %ld (expected %lu)",
                    ret, iov[0].iov_len + iov[1].iov_len);
        }

        next_msg->next_send_pkt++;
        if (next_msg->next_send_pkt >= next_msg->num_pkts) {
            list_del_from(&send_queue, &next_msg->sq_link);
            tt_record("node-%d: removed message to node-%d from send_queue",
                    c.local_rank, peer);
        }

        // Implement packet pacing to avoid pushing too many packets to the
        // network layer.
        now = rdtsc();
        uint32_t drain_us =
                queue_estimator.packetQueued(ret + UDP_IP_HDR_SIZE, now);
        if (drain_us > 1) {
            // Sleep until the transmit queue is almost empty.
            tt_record("about to sleep %u us", drain_us - 1);
            rt::Sleep(drain_us - 1);
            idle_cyc += rdtsc() - now;
        }
    }
    total_cyc = rdtsc() - total_cyc;
    uint64_t busy_cyc = total_cyc - idle_cyc;
    tt_record("node-%d: TX thread load factor 0.%u, busy %u, idle %u",
            c.local_rank, busy_cyc * 100 / total_cyc, busy_cyc, idle_cyc);
}

bool
udp_shuffle(RunBenchOptions& opts, Cluster& c, shuffle_op& op)
{
    if (opts.policy != ShufflePolicy::LRPT) {
        log_err("only LRPT policy is implemented");
        return false;
    }
    op.udp_send_ready = std::make_unique<rt::Semaphore>(0);

    // @out_msgs keeps track of the status of each outbound message and is
    // shared between the TX and RX threads.
    std::vector<udp_out_msg> out_msgs;
    out_msgs.reserve(c.num_nodes);
    for (int i = 0; i < c.num_nodes; i++) {
        size_t num_pkts = (op.out_bufs[i].len + MAX_PAYLOAD - 1) / MAX_PAYLOAD;
        if (i == c.local_rank) {
            num_pkts = 0;
        }
        out_msgs.emplace_back(i, num_pkts);
    }

    // Copy the message destined to itself directly.
    int self = c.local_rank;
    op.in_bufs[self].addr = op.next_inmsg_addr.fetch_add(op.out_bufs[self].len);
    op.in_bufs[self].len = op.out_bufs[self].len;
    memcpy(op.in_bufs[self].addr, op.out_bufs[self].addr, op.in_bufs[self].len);

    // Spin up a thread to handle the receive-side logic of shuffle; the rest of
    // this method will handle the send-side logic.
    rt::Thread rx_main([&] {
        udp_rx_main(c, op, opts.udp_port, out_msgs);
    });

    // Execute the send-side logic and wait for the receive thread to finish.
    udp_tx_main(c, op, opts.udp_port, out_msgs);
    rx_main.Join();

    return true;
}