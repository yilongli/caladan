#pragma once

#include <cstdio>
#include <string>
#include <vector>

#include "net.h"

inline void parse_type(const char *s, char **end, int *value)
{
	*value = strtol(s, end, 0);
}

inline void parse_type(const char *s, char **end, size_t *value)
{
	*value = strtol(s, end, 0);
}

inline void parse_type(const char *s, char **end, int64_t *value)
{
	*value = strtoll(s, end, 0);
}

inline void parse_type(const char *s, char **end, double *value)
{
	*value = strtod(s, end);
}

/**
 * parse() - Parse a value of a particular type from an argument word.
 * @word:      An argument word.
 * @value:     The parsed value corresponding to @word is stored here,
 *             if the function completes successfully.
 * @format:    Name of option being parsed (for use in error messages).
 * @type_name: Human-readable name for ValueType (for use in error messages).
 * Return:     Nonzero means success, zero means an error occurred (and a
 *             message was printed).
 */
template<typename ValueType>
int parse(const char *word, ValueType *value, const char *option,
        const char *type_name)
{
	ValueType num;
	char *end;

	parse_type(word, &end, &num);
	if (*end != 0) {
		printf("Bad value '%s' for %s; must be %s\n",
				word, option, type_name);
		return 0;
	}
	*value = num;
	return 1;
}

/**
 * Options controlling the basic behavior of the experiment. These options are
 * passed in as command-line arguments and remain unchanged over the lifetime
 * of the program.
 */
struct CommandLineOptions {

    explicit CommandLineOptions()
        : num_nodes(-1)
        , local_ip()
        , master_node()
        , log_file(stdout)
    {}

    /// Number of nodes in the experiment.
    int num_nodes;

    /// IP address of this node.
    uint32_t local_ip;

    /// Network address of the master node in the cluster.
    netaddr master_node;

    /// File to write the log messages into.
    FILE* log_file;

    void parse_args(int argc, char* argv[]);
};

/**
 * Options for command "gen_workload".
 */
struct GenWorkloadOptions {

    /// Seed value for a random number generator; this is used to generate
    /// deterministic workloads.
    unsigned rand_seed;

    /// Average size of the shuffle messages, in bytes.
    size_t avg_message_size;

    /// True means a Zipfian distribution will be used to generate the random
    /// data; otherwise, a Guassian distribution is used.
    bool zipf_dist;

    /// Control the skewness of the random data (and the shuffle message sizes
    /// implicitly).
    double data_skew_factor;

    /// Ratio between the largest partition and the smallest partition.
    /// This option controls the skewness of the input/output partitions.
    double part_skew_factor;

    /// True means the input partitions will be skewed; false, otherwise.
    bool skew_input;

    /// True means the output partitions will be skewed; false, otherwise.
    bool skew_output;

    /// True means we should print the message size matrix to the log file
    /// in the end.
    bool print_to_log;

    explicit GenWorkloadOptions()
        : rand_seed(5689u)
        , avg_message_size()
        , zipf_dist(false)
        , data_skew_factor(1.0)
        , part_skew_factor(1.0)
        , skew_input(false)
        , skew_output(false)
        , print_to_log(false)
    {}

    bool parse_args(std::vector<std::string> words);
};

enum ShufflePolicy {
    HADOOP = 0,
    LOCKSTEP = 1,
    SRPT = 2,
    GRPT = 3,
    GRPF = 4,
};

extern const char* shuffle_policy_str[];

struct RunBenchOptions {

    /// True means TCP should be used to send and receive messages. False means
    /// UDP will be used instead.
    bool tcp_protocol;

    /// True means epoll should be used to monitor incoming data; false means
    /// lightweight user-threads will simply issue blocking IO operations.
    bool use_epoll;

    /// When UDP protocol is selected, this is the port number to send and
    /// receive datagrams.
    uint16_t udp_port;

    /// Network bandwidth available to our shuffle application, in Gbps.
    size_t link_speed;

    /// Policy used to schedule shuffle (e.g., which message to transmit next
    /// and how many bytes to transmit).
    ShufflePolicy policy;

    /// Maximum number of inbound messages that can be active at any time
    /// (default: unlimited). This parameter is typically used with the "Hadoop"
    /// policy to enforce the maximum number of connections a server can accept.
    size_t max_in_msgs;

    /// Maximum number of outbound messages that can be active at any time
    /// (default: unlimited). This parameter is typically used with the "Hadoop"
    /// policy to simulate a fixed number of TCP connections.
    size_t max_out_msgs;

    /// In practice, shuffle messages often need to be divided and transmitted
    /// in segments. This value determines the maximum number of bytes in a
    /// message segment.
    size_t max_seg;

    /// Maximum number of data bytes a UDP packet contains. 0 means as large as
    /// possible (if MTU permits).
    size_t max_payload;

    /// True means skipping the memcpy operation that copies out the payload of
    /// incoming data packets. Only used for performance debugging.
    bool no_rx_memcpy;

    /// Number of times to repeat the experiment.
    size_t times;

    explicit RunBenchOptions()
        : tcp_protocol(true)
        , use_epoll(false)
        , udp_port()
        , link_speed(25)
        , policy(ShufflePolicy::HADOOP)
        , max_in_msgs(10000)
        , max_out_msgs(10000)
        , max_seg(1400)
        , max_payload()
        , no_rx_memcpy(false)
        , times(1)
    {}

    bool parse_args(std::vector<std::string> words);
};

struct TimeSyncOptions {

    /// UDP port number used to send and receive probe messages.
    size_t port;

    /// Duration to run the time sync. protocol, in seconds.
    size_t duration;

    explicit TimeSyncOptions()
        : port(5100)
        , duration(1)
    {}

    bool parse_args(std::vector<std::string> words);
};
