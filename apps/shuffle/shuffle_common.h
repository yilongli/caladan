#pragma once

#include <atomic>
#include <cstddef>

#include "sync.h"

/**
 * Identifies the memory region used to store an inbound or outbound shuffle
 * message.
 */
struct shfl_msg_buf {
    explicit shfl_msg_buf()
        : addr(nullptr)
        , len()
    {}

    explicit shfl_msg_buf(char* addr, size_t len)
        : addr(addr)
        , len(len)
    {}

    char* addr;
    size_t len;
};

/**
 * Keep track the progress of a shuffle operation.
 */
struct shuffle_op {

    explicit shuffle_op()
        : num_nodes(-1)
        , out_msgs()
        , in_msgs()
        , total_tx_bytes()
        , total_rx_bytes()
        , tx_data()
        , rx_data()
        , next_inmsg_addr()
        , acked_out_msgs()
    {}

    int num_nodes;

    std::vector<shfl_msg_buf> out_msgs;

    std::vector<shfl_msg_buf> in_msgs;

    /// Total number of bytes in @tx_data.
    size_t total_tx_bytes;

    /// Total number of bytes in @rx_data.
    size_t total_rx_bytes;

    /// Contiguous memory buffer which holds the data for outbound messages
    /// (must be at least @total_tx_bytes large).
    std::unique_ptr<char> tx_data;

    /// Contiguous memory buffer used to store incoming shuffle data (must be
    /// at least @total_rx_bytes large).
    std::unique_ptr<char> rx_data;

    /// Starting address of the memory region that will be used to store the
    /// next inbound message.
    std::atomic<char*> next_inmsg_addr;

    /// Semaphore used to control the number of outbound messages in progress.
    /// The internal value indicates the number of new outbound messages that
    /// can be initiated.
    std::unique_ptr<rt::Semaphore> acked_out_msgs;
};