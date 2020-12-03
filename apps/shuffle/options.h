#pragma once

#include <cstdio>

#include "container.h"
#include "net.h"

inline void parse_type(const char *s, char **end, int *value)
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
 * Options for command "setup_workload".
 */
struct SetupWorkloadOptions {

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

    /// True means the input partitions will be skewed; false, otherwise.
    bool skew_input;

    /// True means the output partitions will be skewed; false, otherwise.
    bool skew_output;

    explicit SetupWorkloadOptions()
        : rand_seed(5689u)
        , avg_message_size()
        , msg_skew_factor(1.0)
        , part_skew_factor(1.0)
        , skew_input(false)
        , skew_output(false)
    {}

    bool parse_args(rt::vector<rt::string> words);
};

struct RunBenchOptions {

    size_t max_unacked_msgs;

    explicit RunBenchOptions()
        : max_unacked_msgs(1)
    {}

    void parse_args(rt::vector<rt::string> words);
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

    void parse_args(rt::vector<rt::string> words);
};
