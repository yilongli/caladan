#pragma once

#include "options.h"

#include <memory>

#include "net.h"
#include "sync.h"

/**
 * Contains information related to the cluster of nodes used in the experiment.
 */
struct Cluster {

    explicit Cluster()
        : num_nodes()
        , local_ip()
        , local_rank(-1)
        , tcp_server_port()
        , master_node()
        , control_socks()
        , server_list()
        , tcp_socks()
        , tcp_write_mutexes()
    {}

    void init(CommandLineOptions* options);
    void connect_all(uint16_t port);
    void disconnect();

    /// Number of nodes in the experiment.
    int num_nodes;

    /// IP address of this node.
    uint32_t local_ip;

    /// Rank of this node.
    int local_rank;

    /// Port number used to accept incoming TCP connections.
    uint16_t tcp_server_port;

    /// Network address of the master node in the cluster.
    netaddr master_node;

    /// TCP connections dedicated to control messages (e.g., start experiment).
    /// On the master node, there is one connection to each of the follower
    /// nodes; on follower nodes, there is only one connection (i.e., to the
    /// master node).
    std::vector<std::unique_ptr<rt::TcpConn>> control_socks;

    /// IP addresses of all the nodes in the cluster, ordered by rank.
    std::vector<uint32_t> server_list;

    // FIXME: the use of unique_ptr's is not very cache-friendly? should we try to optimize it?

    /// TCP connections to all the nodes in the cluster (except itself).
    std::vector<std::unique_ptr<rt::TcpConn>> tcp_socks;

    /// Mutexes used to synchronize writes to TCP streams (otherwise, we can't
    /// tell ACKs from normal data).
    std::vector<std::unique_ptr<rt::Mutex>> tcp_write_mutexes;
};

bool tcp_cmd(std::vector<std::string>& words, Cluster& cluster);