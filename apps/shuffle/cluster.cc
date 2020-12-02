#include "cluster.h"

extern "C" {
#include <base/log.h>
#include <net/ip.h>
}

#include "thread.h"

void
netaddr_to_str(struct netaddr addr, char* str, size_t len) {
    bzero(str, len);
    snprintf(str, len, "%d.%d.%d.%d:%u", ((addr.ip >> 24) & 0xff),
            ((addr.ip >> 16) & 0xff), ((addr.ip >> 8) & 0xff), (addr.ip & 0xff),
            addr.port);
}

/**
  * Find out which nodes are in the cluster and their ranks.
  *
  * \param options
  *     Command-line options
  */
void
Cluster::init(CommandLineOptions* options)
{
    // Initialize basic cluster information based on the command line options.
    num_nodes = options->num_nodes;
    local_addr = options->local_addr;
    master_addr = options->master_addr;

    // Every node needs a listen queue to accept incoming connections.
    listen_queue.reset(rt::TcpQueue::Listen(local_addr, 4096));
    if (listen_queue == nullptr) {
        panic("couldn't listen for connections");
    }

    assert(server_list.empty());
    bool is_master = (local_addr.ip == master_addr.ip) &&
                     (local_addr.port == master_addr.port);
    if (is_master) {
        // Wait for the other nodes to contact us; assign ranks in order.
        // The incoming connections will be teared down afterwards.
        local_rank = 0;
        server_list.push_back(master_addr);
        rt::vector<std::unique_ptr<rt::TcpConn>> conns;
        for (int i = 1; i < num_nodes; i++) {
            rt::TcpConn* c = listen_queue->Accept();
            if (c == nullptr) {
                panic("couldn't accept a connection");
            }
            server_list.emplace_back();
            c->ReadFull(&server_list.back(), sizeof(netaddr));
            conns.emplace_back(c);
            char addr_str[32];
            netaddr_to_str(server_list[i], addr_str, 32);
            log_info("node-0: registered node-%d (%s)", i, addr_str);
        }

        // Broadcast the server list to all nodes.
        for (auto& conn : conns) {
            conn->WriteFull(server_list.data(), num_nodes * sizeof(netaddr));
        }
    } else {
        // Sign up with the master node to obtain the server list.
        std::unique_ptr<rt::TcpConn> c(rt::TcpConn::Dial({0, 0}, master_addr));
        if (c == nullptr) {
            panic("couldn't reach the master node");
        }
        // Tell the master node about our server port number and receive the
        // server list in return.
        c->WriteFull(&options->local_addr, sizeof(netaddr));
        server_list.resize(num_nodes);
        c->ReadFull(&server_list[0], num_nodes * sizeof(netaddr));

        // Find out its rank within the cluster.
        local_rank = -1;
        for (auto& node : server_list) {
            local_rank++;
            if (node.ip == local_addr.ip && node.port == local_addr.port) {
                break;
            }
        }
        log_info("node-%d: received full server list", local_rank);
    }
}

/**
 * Establish TCP connections between every pair of nodes in the cluster.
 * This is done in a way that the number of client connections is roughly the
 * same as the server connections.
 */
void
Cluster::connect_all()
{
    tcp_socks.clear();
    tcp_socks.resize(num_nodes);
    tcp_write_mutexes.resize(num_nodes);
    int num_out_conns = num_nodes / 2;
    if ((num_nodes % 2 == 0) && (local_rank >= num_nodes / 2)) {
        num_out_conns--;
    }
    int num_in_conns = num_nodes - 1 - num_out_conns;

    auto acceptor_thrd = rt::Thread([&] {
        for (int i = 0; i < num_in_conns; i++) {
            rt::TcpConn* c = listen_queue->Accept();
            if (c == nullptr) {
                panic("couldn't accept a connection");
                return;
            }

            // TODO: the following won't work if our nodes can have the same ip
            netaddr remote_addr = c->RemoteAddr();
            bool found = false;
            for (size_t r = 0; r < server_list.size(); r++) {
                if (remote_addr.ip == server_list[r].ip) {
                    found = true;
                    tcp_socks[r].reset(c);
                    log_info("node-%d: accepted connection from node-%lu",
                            local_rank, r);
                    break;
                }
            }
            if (!found) {
                char addr_str[32];
                netaddr_to_str(remote_addr, addr_str, 32);
                panic("unexpected connection from %s", addr_str);
            }
        }
    });

    for (int i = 1; i <= num_out_conns; i++) {
        int r = (local_rank + i) % num_nodes;
        rt::TcpConn* c = rt::TcpConn::Dial({0, 0}, server_list[r]);
        if (c == nullptr) {
            char addr_str[32];
            netaddr_to_str(server_list[r], addr_str, 32);
            panic("couldn't reach node-%d (%s)", r, addr_str);
        }
        tcp_socks[r].reset(c);
        log_info("node-%d: connected to node-%d", local_rank, r);
    }
    acceptor_thrd.Join();
}