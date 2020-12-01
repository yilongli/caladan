#pragma once

#include "shuffle_common.h"

#include <cstdint>

#include "net.h"
#include "sync.h"

/**
 * Message header used in TCP-based shuffle implementation.
 */
struct tcp_shuffle_msg_hdr {
    /// Non-zero means this is merely an ACK of the outbound message;
    /// zero means a message segment containing the actual data will follow
    /// this header immediately.
    uint16_t is_ack;

    /// Number of bytes in the message segment that follows this header.
    uint16_t seg_size;

    /// Number of bytes in the messages.
    uint32_t msg_size;
} __attribute__((__packed__));

void
tcp_rx_thread_main(shuffle_op* op, int peer, rt::TcpConn* c, rt::Mutex* mutex);

void tcp_tx_thread_main(rt::TcpConn* c, rt::Mutex* mutex, shfl_msg_buf out_msg);

void tcp_shuffle(Cluster &c, shuffle_op &op);