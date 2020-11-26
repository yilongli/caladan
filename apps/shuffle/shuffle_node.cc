extern "C" {
#include <base/log.h>
#include <net/ip.h>
#include <runtime/runtime.h>
#include <runtime/smalloc.h>
#include <runtime/storage.h>
}

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <bits/unique_ptr.h>
#include <vector>
#include <fstream>

#include "net.h"
#include "sync.h"
#include "thread.h"

#include "shuffle_util.h"

/// Number of nodes in the experiment.
static int num_nodes = -1;

/// Network address of this node.
static struct netaddr local_addr;

/// Network address of the master node in the cluster.
static struct netaddr master_addr;

/// Rank of this node.
static int local_rank = -1;

/// Listen queue used to accept incoming TCP connections.
static std::unique_ptr<rt::TcpQueue> listen_queue;

/// Network addresses of all the nodes in the cluster, ordered by rank.
static std::vector<struct netaddr> server_list;

/// TCP connections to all the nodes in the cluster (except itself).
static std::vector<std::unique_ptr<rt::TcpConn>> tcp_socks;

/// File to write the log messages into.
static FILE* log_file = stdout;

void print_help(const char* exec_cmd) {
    printf("Usage: %s [caladan-config] [options]\n\n"
           "Start a node that will participate in the shuffle experiment.\n"
           "The first argument specifies the Caladan runtime config file,\n"
           "and the rest are options. The following options are supported:\n\n"
           "--ifname           Symbolic name of the network interface this node"
           "                   will be using in the experiment; this determines"
           "                   the IP address of the node.\n"
           "--port             Port number this node will be listening on for"
           "                   incoming connections.\n"
           "--num-nodes        Total number of nodes in the experiment.\n"
           "--master-addr      Network address where the master server can be"
           "                   reached, in the form of <ip>:<port>"
           "                   (e.g., 10.10.1.2:5000).\n"
           "--log-file         Path to the file which is used for logging.\n",
           exec_cmd
           );
}

/**
 * Find out which nodes are in the cluster and their ranks.
 */
void
cluster_init()
{
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
        std::vector<std::unique_ptr<rt::TcpConn>> conns;
        for (int i = 1; i < num_nodes; i++) {
            rt::TcpConn* c = listen_queue->Accept();
            if (c == nullptr) {
                panic("couldn't accept a connection");
            }
            server_list.push_back(c->RemoteAddr());
            conns.emplace_back(c);
            log_info("node-0: accepted connection from node-%d (%s)", i,
                    netaddr_to_str(server_list[i]).c_str());
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
 * Parse the command-line arguments to initialize the state of this node.
 *
 * \param words
 *      Command-line arguments.
 */
void
server_init(std::vector<std::string>& words) {
    for (size_t i = 0; i < words.size(); i++) {
        const char *option = words[i].c_str();
        if (strcmp(option, "--ifname") == 0) {
            int local_ip = get_local_ip(words[i+1]);
            if (local_ip < 0)
                panic("Unknown interface '%s'", words[i+1].c_str());
            local_addr.ip = local_ip;
            i++;
        } else if (strcmp(option, "--port") == 0) {
            int local_port;
            if (!parse(words, i+1, &local_port, option, "integer"))
                panic("failed to parse '--port %s'", words[i+1].c_str());
            local_addr.port = local_port;
            i++;
        } else if (strcmp(option, "--num-nodes") == 0) {
            if (!parse(words, i+1, &num_nodes, option, "integer"))
                panic("failed to parse '--num-nodes %s'", words[i+1].c_str());
            i++;
        } else if (strcmp(option, "--master-addr") == 0) {
            int ret = parse_netaddr(words[i+1].c_str(), &master_addr);
            if (ret < 0)
                panic("failed to parse '--master-addr %s'", words[i+1].c_str());
            i++;
        } else if (strcmp(option, "--log-file") == 0) {
            FILE* file = std::fopen(words[i+1].c_str(), "w");
            if (!file)
                panic("failed to open log file '%s'", words[i+1].c_str());
            log_file = file;
            i++;
        } else {
            panic("Unknown option '%s'\n", option);
        }
    }

    if (local_addr.ip == 0) {
        panic("failed to initialize local IP address");
    } else if (local_addr.port == 0) {
        panic("failed to initialize local port number");
    } else if (num_nodes < 0) {
        panic("failed to initialize the number of nodes");
    } else if (master_addr.ip == 0) {
        panic("failed to initialize the address of the master node");
    }

    setlinebuf(log_file);
    rt_log_file = log_file;
}

/**
 * Establish TCP connections between every pair of nodes in the cluster.
 * This is done in a way that the number of client connections is roughly the
 * same as the server connections.
 */
void
connect_alltoall()
{
    tcp_socks.clear();
    tcp_socks.resize(num_nodes);
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

            struct netaddr remote_addr = c->RemoteAddr();
            bool found = false;
            int r = 0;
            for (auto& server_addr : server_list) {
                if (remote_addr.ip == server_addr.ip &&
                    remote_addr.port == server_addr.port) {
                    found = true;
                    tcp_socks[r].reset(c);
                    log_info("node-%d: accept connection from node-%d",
                            local_rank, r);
                }
                r++;
            }
            if (!found) {
                panic("unexpected connection from %u:%u", remote_addr.ip,
                        remote_addr.port);
            }
        }
    });

    for (int i = 1; i <= num_out_conns; i++) {
        int r = (local_rank + i) % num_nodes;
        rt::TcpConn* c = rt::TcpConn::Dial({0, 0}, server_list[r]);
        if (c == nullptr) {
            panic("couldn't reach node-%d", r);
        }
        tcp_socks[r].reset(c);
        log_info("node-%d: establish connection to node-%d", local_rank, r);
    }
    acceptor_thrd.Join();
}

void
real_main(void* arg) {
    auto& words = *static_cast<std::vector<std::string>*>(arg);

    server_init(words);
    cluster_init();
    connect_alltoall();

    // Test all-to-all communication
    // FIXME: use epoll?
    int sum = local_rank;
    for (auto& tcp_sock : tcp_socks) {
        if (tcp_sock) {
            tcp_sock->WriteFull(&local_rank, sizeof(local_rank));
        }
    }
    for (auto& tcp_sock : tcp_socks) {
        if (tcp_sock) {
            int r;
            tcp_sock->ReadFull(&r, sizeof(r));
            sum += r;
        }
    }
    log_info("shuffle: sum of all ranks is %d", sum);
}

int main(int argc, char* argv[]) {
    int ret;

	if (((argc >= 2) && (strcmp(argv[1], "--help") == 0)) || (argc == 1)) {
		print_help(argv[0]);
		return 0;
	}

    std::vector<std::string> words;
    for (int i = 2; i < argc; i++) {
        words.emplace_back(argv[i]);
    }

    ret = runtime_init(argv[1], real_main, &words);
    if (ret) {
        panic("failed to start Caladan runtime");
    }

    return 0;
}
