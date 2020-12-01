#include "options.h"

#include <cstring>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <unistd.h>

extern "C" {
#include <base/log.h>
}

/**
 * Use ioctl to obtain the IP address of a local network interface.
 *
 * \param ifname
 *      Symbolic name of the network interface.
 * \return
 *      IP address of the interface on success; -1 on failure.
 */
int get_local_ip(const char* ifname) {
    struct ifreq ifr = {};
    size_t len = std::strlen(ifname);
    std::memcpy(ifr.ifr_name, ifname, len);
    ifr.ifr_name[len + 1] = 0;
    if (len >= sizeof(ifr.ifr_name)) {
        return -1;
    }

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        return -1;
    }

    if (ioctl(fd, SIOCGIFADDR, &ifr) == -1) {
        close(fd);
        return -1;
    }
    return be32toh(((struct sockaddr_in*)&ifr.ifr_addr)->sin_addr.s_addr);
}

int
parse_netaddr(const char* str, struct netaddr* addr) {
    uint8_t a, b, c, d;
    uint16_t port;
    if (sscanf(str, "%hhu.%hhu.%hhu.%hhu:%hu", &a, &b, &c, &d, &port) != 5) {
        return -1;
    }
    addr->ip = (((uint32_t) a << 24) | ((uint32_t) b << 16) |
                ((uint32_t) c << 8) | (uint32_t) d);
    addr->port = port;
    return 0;
}

/**
 * Parse the command-line arguments to initialize the common options.
 *
 * \param argc
 *      Number of words in @argv.
 * \param argv
 *      Command-line arguments.
 */
void
CommandLineOptions::parse_args(int argc, char* argv[]) {
    for (size_t i = 0; i < argc; i++) {
        const char *option = argv[i];
        if (strcmp(option, "--ifname") == 0) {
            int local_ip = get_local_ip(argv[i+1]);
            if (local_ip < 0)
                panic("Unknown interface '%s'", argv[i+1]);
            local_addr.ip = local_ip;
            i++;
        } else if (strcmp(option, "--port") == 0) {
            int local_port;
            if (!parse(argv[i+1], &local_port, option, "integer"))
                panic("failed to parse '--port %s'", argv[i+1]);
            local_addr.port = local_port;
            i++;
        } else if (strcmp(option, "--num-nodes") == 0) {
            if (!parse(argv[i+1], &num_nodes, option, "integer"))
                panic("failed to parse '--num-nodes %s'", argv[i+1]);
            i++;
        } else if (strcmp(option, "--master-addr") == 0) {
            int ret = parse_netaddr(argv[i+1], &master_addr);
            if (ret < 0)
                panic("failed to parse '--master-addr %s'", argv[i+1]);
            i++;
        } else if (strcmp(option, "--log-file") == 0) {
            FILE* file = std::fopen(argv[i+1], "w");
            if (!file)
                panic("failed to open log file '%s'", argv[i+1]);
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
}

/**
 * Parse the arguments for the "setup_workload" command.
 *
 * \param words
 *      Each entry represents one word of the command, like argc/argv.
 */
void
SetupWorkloadOptions::parse_args(rt::vector<rt::string> words)
{
    assert(words[0] == "setup_workload");
    for (size_t i = 0; i < words.size(); i++) {
        const char *option = words[i].c_str();
        if (strcmp(option, "--seed") == 0) {
            int seed;
            if (!parse(words[i+1].c_str(), &seed, option, "integer"))
                panic("failed to parse '%s %s'", option, words[i + 1].c_str());
            rand_seed = seed;
            i++;
        } else if (strcmp(option, "--avg-msg-size") == 0) {
            int size;
            if (!parse(words[i+1].c_str(), &size, option, "integer"))
                panic("failed to parse '%s %s'", option, words[i+1].c_str());
            avg_message_size = size;
            i++;
        } else if (strcmp(option, "--msg-skew-factor") == 0) {
            if (!parse(words[i+1].c_str(), &msg_skew_factor, option, "double"))
                panic("failed to parse '%s %s'", option, words[i+1].c_str());
            i++;
        } else if (strcmp(option, "--part-skew-factor") == 0) {
            if (!parse(words[i+1].c_str(), &part_skew_factor, option, "double"))
                panic("failed to parse '%s %s'", option, words[i+1].c_str());
            i++;
        } else if (strcmp(option, "--skew-input") == 0) {
            skew_input = true;
        } else if (strcmp(option, "--skew-output") == 0) {
            skew_output = true;
        } else {
            panic("Unknown option '%s'\n", option);
        }
    }
}
