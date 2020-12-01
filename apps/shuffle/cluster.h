#pragma once

#include "options.h"

#include <memory>

#include "container.h"
#include "net.h"
#include "sync.h"

/**
 * Contains information related to the cluster of nodes used in the experiment.
 */
struct Cluster {

    explicit Cluster()
        : num_nodes()
        , local_addr()
        , master_addr()
        , local_rank(-1)
        , listen_queue()
        , server_list()
        , tcp_socks()
        , tcp_write_mutexes()
    {}

    void init(CommandLineOptions* options);
    void connect_all();

    /// Number of nodes in the experiment.
    int num_nodes;

    /// Network address of this node.
    struct netaddr local_addr;

    /// Network address of the master node in the cluster.
    struct netaddr master_addr;

    /// Rank of this node.
    int local_rank;

    /// Listen queue used to accept incoming TCP connections.
    std::unique_ptr<rt::TcpQueue> listen_queue;

    /// Network addresses of all the nodes in the cluster, ordered by rank.
    rt::vector<struct netaddr> server_list;

    /// TCP connections to all the nodes in the cluster (except itself).
    rt::vector<std::unique_ptr<rt::TcpConn>> tcp_socks;

    /// Mutexes used to synchronize writes to TCP streams (otherwise, we can't
    /// tell ACKs from normal data).
    rt::vector<std::unique_ptr<rt::Mutex>> tcp_write_mutexes;
};