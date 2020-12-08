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

    /// Control the skewness of the shuffle message sizes.
    double msg_skew_factor;

    /// Ratio between the largest partition and the smallest partition.
    /// This option controls the skewness of the input/output partitions.
    double part_skew_factor;

    /// True means the message sizes will be skewed; false, otherwise.
    /// This option is set to true when option "--msg-skew-factor" is present.
    bool skew_msg;

    /// True means the input partitions will be skewed; false, otherwise.
    bool skew_input;

    /// True means the output partitions will be skewed; false, otherwise.
    bool skew_output;

    explicit GenWorkloadOptions()
        : rand_seed(5689u)
        , avg_message_size()
        , msg_skew_factor(1.0)
        , part_skew_factor(1.0)
        , skew_msg(false)
        , skew_input(false)
        , skew_output(false)
    {}

    bool parse_args(std::vector<std::string> words);
};

enum ShufflePolicy {
    HADOOP = 0,
    LOCKSTEP = 1,
    SRPT = 2,
    LRPT = 3,
};

struct RunBenchOptions {

    /// True means TCP should be used to send and receive messages. False means
    /// UDP will be used instead.
    bool tcp_protocol;

    /// True means epoll should be used to monitor incoming data; false means
    /// lightweight user-threads will simply issue blocking IO operations.
    bool use_epoll;

    /// Policy used to schedule shuffle (e.g., which message to transmit next
    /// and how many bytes to transmit).
    ShufflePolicy policy;

    /// Maximum of outbound messages which can be in progress at any time.
    /// Only used when the policy is HADOOP.
    size_t max_unacked_msgs;

    /// In practice, shuffle messages often need to be divided and transmitted
    /// in segments. This value determines the maximum number of bytes in a
    /// message segment.
    size_t max_seg;

    /// Number of times to repeat the experiment.
    size_t times;

    explicit RunBenchOptions()
        : tcp_protocol(true)
        , use_epoll(false)
        , policy(ShufflePolicy::HADOOP)
        , max_unacked_msgs(1)
        , max_seg(1400)
        , times(0)
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
