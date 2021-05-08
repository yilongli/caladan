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

    /// 0 - message segment
    /// 1 - ACK
    /// 2 - PULL
    uint16_t tag : 2;

    /// When @is_ack is non-zero, this value is the index of the first packet
    /// that has not been received.
    uint16_t ack_no : 14;

    // TODO: redesign the layout of this struct; maybe use union?
    /// When @is_ack is non-zero, this value is the number of packets the sender
    /// is allowed to send.
    uint16_t grant_limit;

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