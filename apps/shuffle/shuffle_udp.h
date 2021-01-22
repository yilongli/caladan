#pragma once

#include "shuffle_common.h"
#include "cluster.h"

#include <cstdint>

#include "net.h"
#include "sync.h"

/**
 * Message header used in UDP-based shuffle implementation.
 */
struct udp_shuffle_msg_hdr {
    /// Unique identifier of the shuffle operation.
    int16_t op_id;

    /// Rank of the message sender within the cluster.
    int16_t peer;

    /// Non-zero means this is an ACK of the outbound message; zero means this
    /// is a message segment.
    uint16_t is_ack : 1;

    /// When @is_ack is non-zero, this value is the index of the first packet
    /// that has not been received.
    uint16_t ack_no : 15;

    /// --------------------------------------------------------
    /// The following members are only used when @is_ack is zero.
    /// --------------------------------------------------------

    /// Number of bytes in the message segment that follows this header.
    uint16_t seg_size;

    /// Number of bytes in the messages.
    uint32_t msg_size;

    /// Start position of this segment within the message.
    uint32_t start;
} __attribute__((__packed__));

void udp_shuffle_init(RunBenchOptions& opts, Cluster& c, shuffle_op& op);
bool udp_shuffle(RunBenchOptions& opts, Cluster &c, shuffle_op &op);