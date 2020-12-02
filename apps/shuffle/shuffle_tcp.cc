#include "shuffle_tcp.h"

#include <atomic>

#include "container.h"
#include "thread.h"

void tcp_rx_thread_main(shuffle_op* op, int peer, rt::TcpConn* c,
        rt::Mutex* mutex)
{
    size_t msg_size = 0;
    size_t rx_bytes = 0;
    char* buf = nullptr;
    tcp_shuffle_msg_hdr msg_hdr;

    // Loop until we have received both the inbound message and the ack for the
    // outbound message.
    bool rx_complete = false;
    bool outmsg_acked = false;
    while (!rx_complete || !outmsg_acked) {
        c->ReadFull(&msg_hdr, sizeof(msg_hdr));
        if (msg_hdr.is_ack) {
            outmsg_acked = true;
            op->acked_out_msgs.Up();
        } else {
            if (!buf) {
                // This is the first segment of the inbound message.
                msg_size = msg_hdr.msg_size;
                buf = op->next_inmsg_addr.fetch_add(msg_size,
                        std::memory_order_relaxed);
            }
            c->ReadFull(buf + rx_bytes, msg_hdr.seg_size);
            rx_bytes += msg_hdr.seg_size;
            if (rx_bytes == msg_size) {
                rx_complete = true;
                op->in_msgs[peer].addr = buf;
                op->in_msgs[peer].len = msg_size;
            }
        }
    }

    // Acknowledge the inbound message.
    msg_hdr = {
        .is_ack = 1,
        .seg_size = 0,
        .msg_size = 0,
    };
    rt::ScopedLock<rt::Mutex> lock_write(mutex);
    c->WriteFull(&msg_hdr, sizeof(msg_hdr));
}

void tcp_tx_thread_main(rt::TcpConn* c, rt::Mutex* mutex, shfl_msg_buf out_msg)
{
    // FIXME: make max_seg_size an option?
    static const size_t MAX_SEG_SIZE = 1400;
    size_t tx_bytes = 0;
    while (tx_bytes < out_msg.len) {
        size_t seg_size = std::min(out_msg.len - tx_bytes, MAX_SEG_SIZE);
        rt::ScopedLock<rt::Mutex> lock_write(mutex);
        c->WriteFull(out_msg.addr + tx_bytes, seg_size);
        tx_bytes += seg_size;
    }
}

void tcp_shuffle(Cluster& c, shuffle_op& op)
{
    // FIXME: how to pass in max_unacked_msgs?
//    for (int i = 0; i < opts.max_unacked_msgs; i++) {
//        op.acked_out_msgs.Up();
//    }

    // step 1. issue a bunch of read uthreads
    rt::vector<rt::Thread> rx_threads;
    rx_threads.reserve(c.num_nodes);
    for (int peer = 0; peer < c.num_nodes; peer++) {
        if (peer != c.local_rank) {
            rt::Thread rx_thread([&] {
                tcp_rx_thread_main(&op, peer, c.tcp_socks[peer].get(),
                        c.tcp_write_mutexes[peer].get());
            });
            rx_threads.emplace_back(std::move(rx_thread));
        }
    }

    // step 2: shuffle the order of out msgs, keep X out msgs concurrently.
    rt::vector<int> peers(c.num_nodes);
    // TODO: shuffle @peers
    for (int i = 0; i < c.num_nodes; i++) {
        peers[i] = i;
    }

    while (!peers.empty()) {
        // Block until we are allowed to initiate another outbound message.
        op.acked_out_msgs.Down();

        // Start a new uthread to send the next outbound message.
        int peer = peers.back();
        peers.pop_back();
        rt::Thread tx_thread([&] {
            tcp_tx_thread_main(c.tcp_socks[peer].get(),
                    c.tcp_write_mutexes[peer].get(),
                    op.out_msgs[peer]);
        });

        // Decouple thread execution from the local thread object. Any allocated
        // resources related to the uthread will be freed by the runtime once
        // the thread exits.
        tx_thread.Detach();
    }

    // step 3. wait for all rx threads to finish, which means all out msgs are
    // acked and all in msgs are received
    while (!rx_threads.empty()) {
        rx_threads.back().Join();
        rx_threads.pop_back();
    }
}