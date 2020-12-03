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
        , local_ip()
        , local_rank(-1)
        , tcp_server_port()
        , master_node()
        , bootstrap_queue()
        , listen_queue()
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

    /// Listen queue used to accept incoming TCP connections at cluster startup.
    /// Only exist on the master node.
    std::unique_ptr<rt::TcpQueue> bootstrap_queue;

    /// Listen queue used to accept incoming TCP connections in response to
    /// the "tcp connect" command.
    std::unique_ptr<rt::TcpQueue> listen_queue;

    /// IP addresses of all the nodes in the cluster, ordered by rank.
    rt::vector<uint32_t> server_list;

    /// TCP connections to all the nodes in the cluster (except itself).
    rt::vector<std::unique_ptr<rt::TcpConn>> tcp_socks;

    /// Mutexes used to synchronize writes to TCP streams (otherwise, we can't
    /// tell ACKs from normal data).
    rt::vector<std::unique_ptr<rt::Mutex>> tcp_write_mutexes;
};

bool tcp_cmd(rt::vector<rt::string>& words, Cluster& cluster);