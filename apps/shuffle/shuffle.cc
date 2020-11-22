extern "C" {
#include <base/log.h>
#include <net/ip.h>
#include <runtime/runtime.h>
#include <runtime/smalloc.h>
#include <runtime/storage.h>
}

#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <bits/unique_ptr.h>
#include <vector>
#include <algorithm>
#include <fstream>

#include "net.h"
#include "sync.h"
#include "thread.h"

/// Port listened on by shuffle servers.
static const int SHUFFLE_SERVER_PORT = 5000;

/// Number of servers in the experiment.
static int num_servers = -1;

/// IP address of this server.
static uint32_t local_ip = 0;

/// Rank of this server.
static int local_rank = -1;

/// IP addresses of all the servers in the cluster, ordered by rank.
std::vector<uint32_t> server_list;

/// TCP connections to all the servers in the cluster (except itself).
std::vector<std::unique_ptr<rt::TcpConn>> tcp_conn;

void print_help() {
    printf("Usage: shuffle <CALADAN_CONFIG> <SHUFFLE_CONFIG> <IFNAME>\n\n"
           "Start a shuffle server that will participate in the experiment.\n\n"
           "CALADAN_CONFIG: Configure file for the Caladan runtime.\n"
           "SHUFFLE_CONFIG: Configure file for the shuffle experiment.\n"
           "IFNAME:         Network interface to use in the experiment.\n"
           );
}

static int str_to_ip(const char* str) {
    uint8_t a, b, c, d;
    if (sscanf(str, "%hhu.%hhu.%hhu.%hhu", &a, &b, &c, &d) != 4) {
        return -1;
    }
    return MAKE_IP_ADDR(a, b, c, d);
}

/**
 * Use ioctl to obtain the IP address of a local network interface.
 *
 * \param ifname
 *      Symbolic name of the network interface.
 * \return
 *      IP address of the interface.
 */
static uint32_t get_local_ip(const std::string ifname) {
    struct ifreq ifr = {};
    ifname.copy(ifr.ifr_name, ifname.length());
    ifr.ifr_name[ifname.length() + 1] = 0;
    if (ifname.length() >= sizeof(ifr.ifr_name)) {
        panic("interface name %s too long", ifname.c_str());
    }

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        panic("failed to create socket: %s", strerror(errno));
    }

    if (ioctl(fd, SIOCGIFADDR, &ifr) == -1) {
        char* error = strerror(errno);
        close(fd);
        panic("Failed to obtain IP address: %s", error);
    }
    return be32toh(((struct sockaddr_in*)&ifr.ifr_addr)->sin_addr.s_addr);
}

struct Node {
    int rank;

    uint32_t ip;

    rt::TcpConn* tcp_conn;
};

void
real_main(void* arg) {
    char** argv = static_cast<char**>(arg);
    char* cfg_path = argv[0];

    // Parse the shuffle config file to get the server list.
    std::ifstream cfg_file(cfg_path);
    std::string line;
    while (std::getline(cfg_file, line)) {
        int addr = str_to_ip(line.c_str());
        if (addr > 0) {
            server_list.push_back(addr);
        }
    }
    std::sort(server_list.begin(), server_list.end());
    num_servers = server_list.size();

    // Determine the rank of this server within the cluster.
    std::string ifname(argv[1]);
    local_ip = get_local_ip(ifname);
    for (size_t i = 0; i < server_list.size(); i++) {
        if (server_list[i] == local_ip) {
            local_rank = i;
            break;
        }
    }

    char ip_str[IP_ADDR_STR_LEN];
    ip_addr_to_str(local_ip, ip_str);
    log_info("shuffle: num_servers %d, rank %d (%s)", num_servers, local_rank,
            ip_str);

    // Every server needs a listen queue to accept incoming connections.
    std::unique_ptr<rt::TcpQueue> listen_queue(
            rt::TcpQueue::Listen({0, SHUFFLE_SERVER_PORT}, 4096));
    if (listen_queue == nullptr) {
        panic("couldn't listen for connections");
    }

    // Setup the all-to-all connections between the servers.
    tcp_conn.resize(num_servers);
    int num_out_conns = num_servers / 2;
    if ((num_servers % 2 == 0) && (local_rank >= num_servers / 2)) {
        num_out_conns--;
    }
    int num_in_conns = num_servers - 1 - num_out_conns;

    auto acceptor_thrd = rt::Thread([&] {
        for (int i = 0; i < num_in_conns; i++) {
            rt::TcpConn* c = listen_queue->Accept();
            if (c == nullptr) {
                panic("couldn't accept a connection");
            }

            uint32_t remote_ip = c->RemoteAddr().ip;
            auto it = std::lower_bound(server_list.begin(), server_list.end(),
                    remote_ip);
            if ((it != server_list.end()) && (*it == remote_ip)) {
                size_t r = std::distance(server_list.begin(), it);
                tcp_conn[r].reset(c);
                log_info("shuffle: accept connection from rank %lu", r);
            } else {
                panic("unexpected connection from %u", remote_ip);
            }
        }
    });

    for (int i = 0; i < num_out_conns; i++) {
        int r = (local_rank + i) % num_servers;
        struct netaddr raddr = {server_list[r], SHUFFLE_SERVER_PORT};
        rt::TcpConn* c = rt::TcpConn::Dial({0, 0}, raddr);
        if (c == nullptr) {
            panic("couldn't reach server %d", r);
        }
        tcp_conn[r].reset(c);
        log_info("shuffle: establish connection to rank %d", r);
    }
    acceptor_thrd.Join();

//    std::vector<Node> nodes(num_servers);
//    nodes[0] = {
//        .rank = 0,
//        .ip = master_ip,
//        .tcp_conn = nullptr,
//    };
//    if (local_rank == 0) {
//        // Accept one connection from each of the other nodes.
//        std::vector<uint32_t> server_list(num_servers);
//        server_list[0] = master_ip;
//        for (int i = 1; i < num_servers; i++) {
//            rt::TcpConn* c = listen_queue->Accept();
//            if (c == nullptr) {
//                panic("couldn't accept a connection");
//            }
//
//            int remote_rank;
//            c->ReadFull(&remote_rank, sizeof(remote_rank));
//            server_list[remote_rank] = c->RemoteAddr().ip;
//            nodes[remote_rank].tcp_conn = c;
//
//            char ip_str[IP_ADDR_STR_LEN];
//            ip_addr_to_str(c->RemoteAddr().ip, ip_str);
//            log_info("shuffle: accept TCP connection from ip %s", ip_str);
//        }
//
//        // Broadcast the addresses of all the participating servers.
//        for (int i = 1; i < num_servers; i++) {
//            nodes[i].tcp_conn->WriteFull(server_list.data(),
//                    num_servers * sizeof(server_list[0]));
//        }
//    } else {
//        rt::TcpConn* c = rt::TcpConn::Dial({0, 0}, master_server);
//        if (c == nullptr) {
//            panic("couldn't reach the master server");
//        }
//        nodes[0].tcp_conn = c;
//
//        c->WriteFull(&local_rank, sizeof(local_rank));
//        for (int i = 0; i < num_servers; i++) {
//            uint32_t ip;
//            c->ReadFull(&ip, sizeof(ip));
//            nodes[i].rank = i;
//            nodes[i].ip = ip;
//            log_info("shuffle: rank %d, ip %u", nodes[i].rank, nodes[i].ip);
//        }
//    }
}

int main(int argc, char* argv[]) {
    int ret;

    if (argc != 4) {
        print_help();
        return -EINVAL;
    }

    ret = runtime_init(argv[1], real_main, &argv[2]);
    if (ret) {
        panic("failed to start Caladan runtime");
    }

    return 0;
}
