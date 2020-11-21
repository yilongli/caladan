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
#include <algorithm>

#include "net.h"
#include "sync.h"
#include "thread.h"

/// Port listened on by shuffle servers.
static const int SHUFFLE_SERVER_PORT = 5000;

/// Number of servers in the experiment.
static int num_servers = -1;

/// Rank of the this server.
static int local_rank = -1;

/// Network address of the master server.
static netaddr master_server;

void print_help() {
    printf("Usage: shuffle <CONFIG> <SERVERS> <RANK> <MASTER_IP>\n\n"
           "Start a shuffle server that will participate in the experiment.\n\n"
           "CONFIG:     Configure file for the Caladan runtime.\n"
           "SERVERS:    Number of servers used in the experiment.\n"
           "RANK:       Unique identifier of this server within the group,\n"
           "            which must be an integer within [0, SERVERS).\n"
           "MASTER_IP:  IPv4 address of the first server (i.e., the server\n"
           "            with rank 0), which is responsible for orchestrating\n"
           "            the experiment.\n"
           );
}

static int str_to_ip(const char* str, uint32_t* addr) {
    uint8_t a, b, c, d;
    if (sscanf(str, "%hhu.%hhu.%hhu.%hhu", &a, &b, &c, &d) != 4) {
        return -EINVAL;
    }

    *addr = MAKE_IP_ADDR(a, b, c, d);
    return 0;
}

struct Node {
    int rank;

    uint32_t ip;

    rt::TcpConn* tcp_conn;
};

void
real_main(void* arg) {
    char** argv = static_cast<char**>(arg);
    log_info("shuffle: num_servers %s, rank %s, master_server %s",
            argv[0], argv[1], argv[2]);

    // Parse arguments.
    num_servers = atoi(argv[0]);
    local_rank = atoi(argv[1]);
    uint32_t master_ip;
    if (str_to_ip(argv[2], &master_ip)) {
        log_err("failed to parse the master ip");
        return;
    }
    master_server = {master_ip, SHUFFLE_SERVER_PORT};

    // Every server needs a listen queue to accept incoming connections.
    std::unique_ptr<rt::TcpQueue> listen_queue(
            rt::TcpQueue::Listen({0, SHUFFLE_SERVER_PORT}, 4096));
    if (listen_queue == nullptr) {
        panic("couldn't listen for connections");
    }

    std::vector<Node> nodes(num_servers);
    nodes[0] = {
        .rank = 0,
        .ip = master_ip,
        .tcp_conn = nullptr,
    };
    if (local_rank == 0) {
        // Accept one connection from each of the other nodes.
        std::vector<uint32_t> server_list(num_servers);
        server_list[0] = master_ip;
        for (int i = 1; i < num_servers; i++) {
            rt::TcpConn* c = listen_queue->Accept();
            if (c == nullptr) {
                panic("couldn't accept a connection");
            }

            int remote_rank;
            c->ReadFull(&remote_rank, sizeof(remote_rank));
            server_list[remote_rank] = c->RemoteAddr().ip;
            nodes[remote_rank].tcp_conn = c;

            char ip_str[IP_ADDR_STR_LEN];
            ip_addr_to_str(c->RemoteAddr().ip, ip_str);
            log_info("shuffle: accept TCP connection from ip %s", ip_str);
        }

        // Broadcast the addresses of all the participating servers.
        for (int i = 1; i < num_servers; i++) {
            nodes[i].tcp_conn->WriteFull(server_list.data(),
                    num_servers * sizeof(server_list[0]));
        }
    } else {
        rt::TcpConn* c = rt::TcpConn::Dial({0, 0}, master_server);
        if (c == nullptr) {
            panic("couldn't reach the master server");
        }
        nodes[0].tcp_conn = c;

        c->WriteFull(&local_rank, sizeof(local_rank));
        for (int i = 0; i < num_servers; i++) {
            uint32_t ip;
            c->ReadFull(&ip, sizeof(ip));
            nodes[i].rank = i;
            nodes[i].ip = ip;
            log_info("shuffle: rank %d, ip %u", nodes[i].rank, nodes[i].ip);
        }
    }

    // Setup the all-to-all connections between the servers.

}

int main(int argc, char* argv[]) {
    int ret;

    if (argc != 5) {
        print_help();
        return -EINVAL;
    }

    ret = runtime_init(argv[1], real_main, &argv[2]);
    if (ret) {
        panic("failed to start Caladan runtime");
    }

    return 0;
}
