#include "shuffle_tcp.h"

extern "C" {
#include <base/log.h>
}

#include <memory>
#include <random>
#include <algorithm>

#include "thread.h"

void tcp_rx_thread_main(Cluster& c, shuffle_op& op, int peer)
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
            log_info("node-%d: received ACK from node-%d", c.local_rank, peer);
            outmsg_acked = true;
            op.acked_out_msgs->Up();
        } else {
            // Normal data segment.
            log_info("node-%d: receiving bytes %lu-%lu from node-%d",
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

    // Issue a bunch of read uthreads
    std::vector<rt::Thread> rx_threads;
    rx_threads.reserve(c.num_nodes);
    for (int r = 0; r < c.num_nodes; r++) {
        if (r != c.local_rank) {
            rt::Thread rx_thread([&, peer = r] {
                tcp_rx_thread_main(c, op, peer);
            });
            rx_threads.emplace_back(std::move(rx_thread));
        }
    }

    // Make sure all outbound messages have been acknowledged and all inbound
    // messages have been received before return.
    while (!rx_threads.empty()) {
        rx_threads.back().Join();
        rx_threads.pop_back();
    }
}


void tcp_tx_thread_main(RunBenchOptions &opts, Cluster &c, shuffle_op &op,
        int peer)
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
        size_t seg_size = std::min(out_msg.len - tx_bytes, opts.max_seg);
        log_info("node-%d: sending bytes %lu-%lu to node-%d", c.local_rank,
                tx_bytes, tx_bytes + seg_size, peer);

        // Lock the TCP stream to avoid data corruption due to the ACK.
        rt::ScopedLock<rt::Mutex> lock_write(mutex.get());
        msg_hdr.seg_size = seg_size;
        conn->WriteFull(&msg_hdr, sizeof(msg_hdr));
        conn->WriteFull(out_msg.addr + tx_bytes, seg_size);
        tx_bytes += seg_size;
    } while (tx_bytes < out_msg.len);
}

bool
tcp_shuffle(RunBenchOptions& opts, Cluster& c, shuffle_op& op)
{
    // Initialize the semaphore
    op.acked_out_msgs = std::make_unique<rt::Semaphore>(opts.max_unacked_msgs);

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

    // FIXME: this is the hadoop policy; how to implement other policies modularly?
    // Shuffle our outbound messages randomly (hoping to reduce hot spots)
    std::vector<int> peers;
    peers.reserve(c.num_nodes);
    for (int i = 0; i < c.num_nodes; i++) {
        if (i != c.local_rank)
            peers.push_back(i);
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(peers.begin(), peers.end(), g);

    // Start the shuffle process; always keep a fixed number of outbound
    // messages in progress.
    while (!peers.empty()) {
        // Block until we are allowed to initiate another outbound message.
        op.acked_out_msgs->Down();

        // Spin up a new uthread to send the next outbound message.
        rt::Thread tx_thread([&, peer = peers.back()] {
            tcp_tx_thread_main(opts, c, op, peer);
        });
        peers.pop_back();

        // Decouple thread execution from the local thread object. Any allocated
        // resources related to the uthread will be freed by the caladan runtime
        // once the thread exits.
        tx_thread.Detach();
    }

    // TODO: maybe fetch result from rx_main to check if it fails???
    rx_main.Join();

    // FIXME: remove this or make this log_info??
//    std::string result(op.rx_data.get(), op.total_rx_bytes);
//    log_info("shuffle result: %s", result.c_str());

    return true;
}