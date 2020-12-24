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
    /// Non-zero means this is an ACK of the outbound message, and the value
    /// is the index of the first packet that has not been received. Zero means
    /// a message segment containing the actual data will follow this header
    /// immediately.
    uint16_t ack_no;

    /// Number of bytes in the message segment that follows this header.
    uint16_t seg_size;

    /// Number of bytes in the messages.
    uint32_t msg_size;

    /// Start position of this segment within the message.
    uint32_t start;

    /// Rank of the message sender within the cluster.
    int16_t peer;

    /// Unique identifier of the shuffle operation.
    int16_t op_id;

} __attribute__((__packed__));

bool udp_shuffle(RunBenchOptions& opts, Cluster &c, shuffle_op &op);