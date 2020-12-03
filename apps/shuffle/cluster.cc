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
    local_ip = options->local_ip;
    master_node = options->master_node;

    assert(server_list.empty());
    bool is_master = (local_ip == master_node.ip);
    if (is_master) {
        bootstrap_queue.reset(rt::TcpQueue::Listen(master_node, 4096));
        if (!bootstrap_queue) {
            panic("master node couldn't listen for connections");
        }

        // Wait for the other nodes to contact us; assign ranks in order.
        // The incoming connections will be teared down afterwards.
        local_rank = 0;
        server_list.push_back(master_node.ip);
        rt::vector<std::unique_ptr<rt::TcpConn>> tmp_conns;
        for (int i = 1; i < num_nodes; i++) {
            rt::TcpConn* c = bootstrap_queue->Accept();
            if (c == nullptr) {
                panic("couldn't accept a connection");
            }
            tmp_conns.emplace_back(c);
            server_list.push_back(c->RemoteAddr().ip);
            char ip_str[32];
            ip_addr_to_str(server_list[i], ip_str);
            log_info("node-0: registered node-%d (%s)", i, ip_str);
        }

        // Broadcast the server list to all nodes.
        for (auto& conn : tmp_conns) {
            const size_t elem_size = sizeof(decltype(server_list)::value_type);
            conn->WriteFull(server_list.data(), num_nodes * elem_size);
        }
    } else {
        // Sign up with the master node to obtain the server list.
        std::unique_ptr<rt::TcpConn> c(rt::TcpConn::Dial({0, 0}, master_node));
        if (c == nullptr) {
            panic("couldn't reach the master node");
        }
        // Tell the master node about our server port number and receive the
        // server list in return.
        server_list.resize(num_nodes);
        const size_t elem_size = sizeof(decltype(server_list)::value_type);
        c->ReadFull(&server_list[0], num_nodes * elem_size);

        // Find out its rank within the cluster.
        local_rank = -1;
        for (size_t i = 0; i < server_list.size(); i++) {
            if (server_list[i] == local_ip) {
                local_rank = i;
                break;
            }
        }
        if (local_rank < 0) {
            panic("failed to determine the rank");
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
Cluster::connect_all(uint16_t port)
{
    // Tear down all previous connections.
    tcp_server_port = port;
    tcp_socks.clear();
    tcp_socks.resize(num_nodes);
    tcp_write_mutexes.resize(num_nodes);

    // Compute the number of inbound and outbound connections.
    int num_out_conns = num_nodes / 2;
    if ((num_nodes % 2 == 0) && (local_rank >= num_nodes / 2)) {
        num_out_conns--;
    }
    int num_in_conns = num_nodes - 1 - num_out_conns;

    // Create a new listen queue to accept incoming connections.
    listen_queue.reset(rt::TcpQueue::Listen({local_ip, tcp_server_port}, 4096));
    if (listen_queue == nullptr) {
        panic("couldn't listen for connections");
    }

    auto acceptor = rt::Thread([&] {
        for (int i = 0; i < num_in_conns; i++) {
            rt::TcpConn* c = listen_queue->Accept();
            if (c == nullptr) {
                panic("couldn't accept a connection");
                return;
            }

            uint32_t peer_ip = c->RemoteAddr().ip;
            bool found = false;
            for (size_t r = 0; r < server_list.size(); r++) {
                if (peer_ip == server_list[r]) {
                    found = true;
                    tcp_socks[r].reset(c);
                    log_info("node-%d: accepted connection from node-%lu",
                            local_rank, r);
                    break;
                }
            }
            if (!found) {
                char addr_str[32];
                netaddr_to_str(c->RemoteAddr(), addr_str, 32);
                panic("unexpected connection from %s", addr_str);
            }
        }
    });

    for (int i = 1; i <= num_out_conns; i++) {
        int r = (local_rank + i) % num_nodes;
        netaddr peer = {server_list[r], tcp_server_port};
        rt::TcpConn* c = rt::TcpConn::Dial({0, 0}, peer);
        if (c == nullptr) {
            char addr_str[32];
            netaddr_to_str(peer, addr_str, 32);
            panic("couldn't reach node-%d (%s)", r, addr_str);
        }
        tcp_socks[r].reset(c);
        log_info("node-%d: connected to node-%d", local_rank, r);
    }
    acceptor.Join();
}

void
Cluster::disconnect()
{
    tcp_server_port = 0;
    tcp_socks.clear();
    tcp_socks.resize(num_nodes);
    listen_queue.reset();
}

/**
 * Parse the arguments of a "verify_tcp" command and execute it.
 *
 * \return
 *      True means success, false means there was an error.
 */
bool
verify_tcp(Cluster& cluster)
{
    // Test all-to-all communication
    int sum = cluster.local_rank;
    for (auto& tcp_sock : cluster.tcp_socks) {
        if (!tcp_sock) continue;
        tcp_sock->WriteFull(&cluster.local_rank, sizeof(cluster.local_rank));
    }
    for (auto& tcp_sock : cluster.tcp_socks) {
        if (tcp_sock) {
            int r;
            tcp_sock->ReadFull(&r, sizeof(r));
            sum += r;
        }
    }
    int expected = (cluster.num_nodes - 1) * cluster.num_nodes / 2;
    if (sum == expected) {
        log_info("verify_tcp: success");
        return true;
    } else {
        log_err("verify_tcp: unexpected sum of ranks %d", sum);
        return false;
    }
}

/**
 * Parse the arguments of a "verify_tcp" command and execute it.
 *
 * \return
 *      True means success, false means there was an error.
 */
bool
tcp_cmd(rt::vector<rt::string>& words, Cluster& cluster)
{
    assert(words[0] == "tcp");
    for (size_t i = 1; i < words.size(); i++) {
        const char *option = words[i].c_str();
        if (words[i] == "connect") {
            int port;
            if (!parse(words[i+1].c_str(), &port, option, "integer")) {
                log_err("failed to parse '%s %s'", option, words[i+1].c_str());
                return false;
            }
            cluster.connect_all(port);
        } else if (words[i] == "verify") {
            verify_tcp(cluster);
        } else if (words[i] == "disconnect") {
            cluster.disconnect();
        } else {
            log_err("Unknown option '%s'\n", words[i].c_str());
            return false;
        }
    }
    return true;
}