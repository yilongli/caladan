#include "shuffle_tcp.h"

extern "C" {
#include <base/log.h>
}

#include <memory>
#include <random>
#include <algorithm>

#include "thread.h"

// FIXME: implement real timetrace; log_info is way too slow (0.7~1.3 us per call!)
namespace {
    inline void
    tt_record(const char* format,
            uint64_t arg0 = 0, uint64_t arg1 = 0, uint64_t arg2 = 0,
            uint64_t arg3 = 0)
    {
//        log_info(format, arg0, arg1, arg2, arg3);
    }
}


/**
 * Receives the inbound message and ACK from a given peer via a TCP connection.
 * Block until both tasks are finished.
 *
 * Note: this method may also write control messages to the TCP stream in order
 * to stop/restart/acknowledge the message sender.
 *
 * \param c
 *      Cluster object.
 * \param op
 *      Shuffle object that keeps track of the progress.
 * \param peer
 *      Rank of the message sender within the cluster.
 */
void tcp_rx_msg(Cluster& c, shuffle_op& op, int peer)
{
    auto& conn = c.tcp_socks[peer];
    auto& mutex = c.tcp_write_mutexes[peer];

    size_t msg_size = 0;
    size_t rx_bytes = 0;
    char* buf = nullptr;
    tcp_shuffle_msg_hdr msg_hdr{};

    // Loop until we have received both the inbound message and the ack for the
    // outbound message.
    bool rx_complete = false;
    bool outmsg_acked = false;
    while (!rx_complete || !outmsg_acked) {
        // Read the next message header.
        conn->ReadFull(&msg_hdr, sizeof(msg_hdr));

        if (msg_hdr.is_ack) {
            // ACK message.
            tt_record("node-%d: received ACK from node-%d", c.local_rank, peer);
            outmsg_acked = true;
            op.acked_out_msgs->Up();
        } else {
            // Normal data segment.
            tt_record("node-%d: receiving bytes %lu-%lu from node-%d",
                    c.local_rank, rx_bytes, rx_bytes + msg_hdr.seg_size, peer);
            if (!buf) {
                // This is the first segment of the inbound message.
                msg_size = msg_hdr.msg_size;
                buf = op.next_inmsg_addr.fetch_add(msg_size,
                        std::memory_order_relaxed);
            }
            conn->ReadFull(buf + rx_bytes, msg_hdr.seg_size);
            rx_bytes += msg_hdr.seg_size;
            if (rx_bytes == msg_size) {
                // Inbound message is complete.
                rx_complete = true;
                op.in_msgs[peer].addr = buf;
                op.in_msgs[peer].len = msg_size;

                // Acknowledge the inbound message.
                tt_record("node-%d: sending ACK to node-%d", c.local_rank,
                        peer);
                msg_hdr.is_ack = 1;
                rt::ScopedLock<rt::Mutex> lock_write(mutex.get());
                conn->WriteFull(&msg_hdr, sizeof(msg_hdr));
            }
        }
    }
}

void
tcp_rx_main(RunBenchOptions& opts, Cluster& c, shuffle_op& op)
{
    // TODO: with "--epoll" we only need one rx uthread(?); make it modular
    if (opts.use_epoll) {
        // FIXME: ???
        log_err("option '--epoll' not implemented!");
    }

    // TODO: how to implement lock-step RX??? the following won't work:
    // - no uthread to receive ACK
    // - no way to tell a unwanted sender to stop (inbound bytes will be queued
    //   at TCP layer and eventually stop the sender, but that could be too slow)
    // finally, for TCP-based implementations, it's better to have equal number
    // of uthreads for different policies (fair comparison)
//    if (opts.policy == ShufflePolicy::LOCKSTEP) {
//        // The first inbound message should come from its right neighbor, etc.
//        for (int r = 1; r < c.num_nodes; r++) {
//            int peer = (c.local_rank + r) % c.num_nodes;
//            tcp_rx_thread_main(c, op, peer);
//        }
//    }

    // FIXME: the following won't achieve real lockstep yet because we don't
    // have a mechanism to tell a sender to stop sending if we are expecting a
    // different sender.
    // FIXME: one solution is to augment the msg_hdr to include STOP/CONTINUE

    // Spawn one read thread for each remote peer in the cluster.
    std::vector<rt::Thread> rx_threads;
    rx_threads.reserve(c.num_nodes);
    for (int r = 0; r < c.num_nodes; r++) {
        if (r != c.local_rank) {
            rt::Thread rx_thread([&, peer = r] {
                tcp_rx_msg(c, op, peer);
            });
            rx_threads.emplace_back(std::move(rx_thread));
        }
    }

    // Make sure all outbound messages are acknowledged and all inbound
    // messages are received before return.
    while (!rx_threads.empty()) {
        rx_threads.back().Join();
        rx_threads.pop_back();
    }
}

/**
 * Sends an outbound message to a given peer via a TCP stream. Block until all
 * the bytes have been transmitted (i.e., written to the TCP stream).
 *
 * \param c
 *      Cluster object.
 * \param op
 *      Shuffle object that keeps track of the progress.
 * \param peer
 *      Rank of the message receiver within the cluster.
 * \param max_seg_size
 *      Maximum number of bytes in each message segment.
 */
void tcp_tx_msg(Cluster &c, shuffle_op &op, int peer, size_t max_seg_size)
{
    auto& conn = c.tcp_socks[peer];
    auto& mutex = c.tcp_write_mutexes[peer];
    auto& out_msg = op.out_msgs[peer];
    tcp_shuffle_msg_hdr msg_hdr = {
        .is_ack = 0,
        .seg_size = 0,
        .msg_size = uint32_t(out_msg.len)
    };

    // The following loop transmits the outbound message in segments. This is
    // necessary to avoid creating huge HOL blocking for the ACK message that
    // is sharing the underlying TCP stream.
    size_t tx_bytes = 0;
    do {
        size_t seg_size = std::min(out_msg.len - tx_bytes, max_seg_size);
        tt_record("node-%d: sending bytes %lu-%lu to node-%d", c.local_rank,
                tx_bytes, tx_bytes + seg_size, peer);

        // Lock the TCP stream to avoid data corruption due to the ACK.
        rt::ScopedLock<rt::Mutex> lock_write(mutex.get());
        msg_hdr.seg_size = seg_size;
        conn->WriteFull(&msg_hdr, sizeof(msg_hdr));
        conn->WriteFull(out_msg.addr + tx_bytes, seg_size);
        tx_bytes += seg_size;
    } while (tx_bytes < out_msg.len);
}

void tcp_tx_main(RunBenchOptions& opts, Cluster& c, shuffle_op& op)
{
    std::vector<int> peers;
    peers.reserve(c.num_nodes);
    for (int i = 1; i < c.num_nodes; i++) {
        peers.push_back((c.local_rank + i) % c.num_nodes);
    }
    size_t max_seg_size = opts.max_seg;
    size_t max_unacked_msgs = opts.max_unacked_msgs;

    // Customize the behavior based on the selected policy.
    if (opts.policy == ShufflePolicy::LOCKSTEP) {
        // No need to chop a message into small pieces when doing lock-step.
        max_seg_size = UINT32_MAX;
        max_unacked_msgs = 1;
    } else if (opts.policy == ShufflePolicy::HADOOP) {
        // Shuffle our outbound messages randomly (hoping to reduce hot spots)
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(peers.begin(), peers.end(), g);
    } else if (opts.policy == ShufflePolicy::SRPT) {
        // Sort the outbound messages in descending order of length (messages
        // are selected from the end of the vector later).
        auto compare = [&op] (int r0, int r1) {
            return op.out_msgs[r0].len > op.out_msgs[r1].len;
        };
        std::sort(peers.begin(), peers.end(), compare);
    }

    // FIXME: can LRPT policy fit in the following framework??? I doubt so since
    // this framework is only suitable for run-to-completion policies? (e.g.,
    // @tcp_tx_msg sends a message to completion before return)

    // Start the shuffle process; always keep a fixed number of outbound
    // messages in progress.
    while (!peers.empty()) {
        // Block until we are allowed to initiate another outbound message.
        op.acked_out_msgs->Down();

        if (max_unacked_msgs == 1) {
            // Optimization: no need to spawn a new thread if we can only have
            // one ongoing message at a time.
            tcp_tx_msg(c, op, peers.back(), max_seg_size);
        } else {
            // Spawn a new uthread to send the next outbound message.
            rt::Thread tx_thread([&, peer = peers.back()] {
                tcp_tx_msg(c, op, peer, max_seg_size);
            });

            // Decouple thread execution from the local thread object.
            // Any allocated resources related to the uthread will be
            // freed by the caladan runtime once the thread exits.
            tx_thread.Detach();
        }

        peers.pop_back();
    }
}

bool
tcp_shuffle(RunBenchOptions& opts, Cluster& c, shuffle_op& op)
{
    // Initialize the semaphore
    op.acked_out_msgs = std::make_unique<rt::Semaphore>(opts.max_unacked_msgs);
    if ((opts.policy == ShufflePolicy::LOCKSTEP) &&
        (opts.max_unacked_msgs > 1)) {
        op.acked_out_msgs = std::make_unique<rt::Semaphore>(1);
    }

    // Copy the message destined to itself directly.
    int self = c.local_rank;
    op.in_msgs[self].addr = op.next_inmsg_addr.fetch_add(op.out_msgs[self].len);
    op.in_msgs[self].len = op.out_msgs[self].len;
    memcpy(op.in_msgs[self].addr, op.out_msgs[self].addr, op.in_msgs[self].len);
    
    // Spin up a thread to handle the receive-side logic of shuffle; the rest of
    // this method will handle the send-side logic.
    rt::Thread rx_main([&] {
        tcp_rx_main(opts, c, op);
    });

    // Execute the send-side logic and wait for the receive thread to finish.
    tcp_tx_main(opts, c, op);
    rx_main.Join();

    // FIXME: remove this or make this log_info??
//    std::string result(op.rx_data.get(), op.total_rx_bytes);
//    log_info("shuffle result: %s", result.c_str());

    return true;
}