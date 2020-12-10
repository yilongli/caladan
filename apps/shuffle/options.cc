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
    for (int i = 0; i < argc; i++) {
        const char *option = argv[i];
        if (strcmp(option, "--ifname") == 0) {
            int ip = get_local_ip(argv[i+1]);
            if (ip < 0)
                panic("Unknown interface '%s'", argv[i+1]);
            local_ip = ip;
            i++;
        } else if (strcmp(option, "--num-nodes") == 0) {
            if (!parse(argv[i+1], &num_nodes, option, "integer"))
                panic("failed to parse '--num-nodes %s'", argv[i+1]);
            i++;
        } else if (strcmp(option, "--master-addr") == 0) {
            int ret = parse_netaddr(argv[i+1], &master_node);
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
            panic("Unknown option '%s'", option);
        }
    }

    if (local_ip == 0) {
        panic("failed to initialize local IP address");
    } else if (num_nodes < 0) {
        panic("failed to initialize the number of nodes");
    } else if (master_node.ip == 0) {
        panic("failed to initialize the address of the master node");
    }
}

/**
 * Parse the arguments for the "gen_workload" command.
 *
 * \param words
 *      Each entry represents one word of the command, like argc/argv.
 * \return
 *      True means success, false means there was an error.
 */
bool
GenWorkloadOptions::parse_args(std::vector<std::string> words)
{
    assert(words[0] == "gen_workload");
    for (size_t i = 1; i < words.size(); i++) {
        const char *option = words[i].c_str();
        if (strcmp(option, "--seed") == 0) {
            int seed;
            if (!parse(words[i+1].c_str(), &seed, option, "integer")) {
                log_err("failed to parse '%s %s'", option, words[i+1].c_str());
                return false;
            }
            rand_seed = seed;
            i++;
        } else if (strcmp(option, "--avg-msg-size") == 0) {
            int size;
            if (!parse(words[i+1].c_str(), &size, option, "integer")) {
                log_err("failed to parse '%s %s'", option, words[i+1].c_str());
                return false;
            }
            avg_message_size = size;
            i++;
        } else if (strcmp(option, "--msg-skew-factor") == 0) {
            if (!parse(words[i+1].c_str(), &msg_skew_factor, option, "double"))
            {
                log_err("failed to parse '%s %s'", option, words[i+1].c_str());
                return false;
            }
            skew_msg = true;
            i++;
        } else if (strcmp(option, "--part-skew-factor") == 0) {
            if (!parse(words[i+1].c_str(), &part_skew_factor, option, "double"))
            {
                log_err("failed to parse '%s %s'", option, words[i+1].c_str());
                return false;
            }
            i++;
        } else if (strcmp(option, "--skew-input") == 0) {
            skew_input = true;
        } else if (strcmp(option, "--skew-output") == 0) {
            skew_output = true;
        } else {
            log_err("Unknown option '%s'", option);
            return false;
        }
    }
    return true;
}

/**
 * Parse the arguments for the "run_bench" command.
 *
 * \param words
 *      Each entry represents one word of the command, like argc/argv.
 * \return
 *      True means success, false means there was an error.
 */
bool
RunBenchOptions::parse_args(std::vector<std::string> words)
{
    assert(words[0] == "run_bench");
    for (size_t i = 1; i < words.size(); i++) {
        const char *option = words[i].c_str();
        if (strcmp(option, "--protocol") == 0) {
            if (words[i+1] == "tcp") {
                tcp_protocol = true;
            } else if (words[i+1] == "udp") {
                tcp_protocol = false;
            } else {
                log_err("Unknown protocol '%s'", words[i+1].c_str());
                return false;
            }
            i++;
        } else if (strcmp(option, "--udp-port") == 0) {
            size_t port;
            if (!parse(words[i + 1].c_str(), &port, option, "unsigned")) {
                log_err("failed to parse '%s %s'", option, words[i+1].c_str());
                return false;
            }
            udp_port = port;
            i++;
        } else if (strcmp(option, "--epoll") == 0) {
            use_epoll = true;
            i++;
        } else if (strcmp(option, "--policy") == 0) {
            if (words[i+1] == "hadoop") {
                policy = ShufflePolicy::HADOOP;
            } else if (words[i+1] == "lockstep") {
                policy = ShufflePolicy::LOCKSTEP;
            } else if (words[i+1] == "LRPT") {
                policy = ShufflePolicy::LRPT;
            } else if (words[i+1] == "SRPT") {
                policy = ShufflePolicy::SRPT;
            } else {
                log_err("Unknown policy '%s'", words[i+1].c_str());
                return false;
            }
            i++;
        } else if (strcmp(option, "--max-unacked") == 0) {
            if (!parse(words[i + 1].c_str(), &max_unacked_msgs, option,
                    "unsigned")) {
                log_err("failed to parse '%s %s'", option, words[i+1].c_str());
                return false;
            }
            i++;
        } else if (strcmp(option, "--max-seg") == 0) {
            if (!parse(words[i+1].c_str(), &max_seg, option, "unsigned")) {
                log_err("failed to parse '%s %s'", option, words[i+1].c_str());
                return false;
            }
            i++;
        } else if (strcmp(option, "--times") == 0) {
            if (!parse(words[i+1].c_str(), &times, option, "unsigned")) {
                log_err("failed to parse '%s %s'", option, words[i+1].c_str());
                return false;
            }
            i++;
        } else {
            log_err("Unknown option '%s'", option);
            return false;
        }
    }
    return true;
}

/**
 * Parse the arguments for the "time_sync" command.
 *
 * \param words
 *      Each entry represents one word of the command, like argc/argv.
 * \return
 *      True means success, false means there was an error.
 */
bool
TimeSyncOptions::parse_args(std::vector<std::string> words)
{
    return true;
}


