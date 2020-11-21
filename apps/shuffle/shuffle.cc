extern "C" {
#include <base/log.h>
#include <runtime/runtime.h>
#include <runtime/smalloc.h>
#include <runtime/storage.h>
}

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "net.h"
#include "sync.h"
#include "thread.h"

/// Port listened on by shuffle servers.
static const int SHUFFLE_SERVER_PORT = 5000;

void print_help() {
    printf("Usage: shuffle <CONFIG> <LOG_DIR> <SERVERS> <RANK> <MASTER_IP>\n\n"
           "Start a shuffle server that will participate in the experiment.\n\n"
           "CONFIG:     Configure file for the Caladan runtime.\n"
           "LOG_DIR:    Log directory shared among all participating servers.\n"
           "SERVERS:    Number of servers used in the experiment.\n"
           "RANK:       Unique identifier of this server within the group\n"
           "            (must be an integer within [0, SERVERS).\n"
           "MASTER_IP:  IPv4 address of the first server (i.e., the server\n"
           "            with rank 0), which is responsible for orchestrating\n"
           "            the experiment.\n"
           );
}

void
real_main(void* arg) {
    char** argv = static_cast<char**>(arg);
    printf("LOG_DIR = %s\n", argv[0]);
    printf("SERVERS = %s\n", argv[1]);
    printf("RANK = %s\n", argv[2]);
    printf("MASTER_IP = %s\n", argv[3]);

//    std::unique_ptr<rt::TcpQueue> listen_queue(
//            rt::TcpQueue::Listen({0, SHUFFLE_SERVER_PORT}, 4096));
//    if (listen_queue == nullptr) {
//        panic("couldn't listen for connections");
//    }
//
//    while (true) {
//        rt::TcpConn* c = q->Accept();
//        if (c == nullptr) panic("couldn't accept a connection");
//        rt::Thread([=] {
//            ServerWorker(std::shared_ptr<rt::TcpConn>(c));
//        }).Detach();
//    }
}

int main(int argc, char* argv[]) {
    int ret;

    if (argc != 6) {
        print_help();
        return -EINVAL;
    }

    ret = runtime_init(argv[1], real_main, &argv[2]);
    if (ret) {
        panic("failed to start Caladan runtime");
    }

    return 0;
}
