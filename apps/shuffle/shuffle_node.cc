extern "C" {
#include <base/log.h>
#include <net/ip.h>
#include <runtime/runtime.h>
#include <runtime/smalloc.h>
}

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <atomic>

#include "thread.h"

#include "cluster.h"
#include "shuffle_tcp.h"
#include "workload.h"

// Note: we don't want any static initializer because they are run before
// starting the caladan runtime; as a result, all the static variables are
// of pointer type.

/// Command-line options.
static CommandLineOptions* cmd_line_opts;

/// Cluster of nodes used in the experiment.
static Cluster* cluster;

/// Current shuffle workload.
static shuffle_op* current_op;

void print_help(const char* exec_cmd) {
    printf("Usage: %s [caladan-config] [options]\n\n"
           "Start a node that will participate in the shuffle experiment.\n"
           "The first argument specifies the Caladan runtime config file,\n"
           "and the rest are options. The following options are supported:\n\n"
           "--ifname           Symbolic name of the network interface this node"
           "                   will be using in the experiment; this determines"
           "                   the IP address of the node.\n"
           "--num-nodes        Total number of nodes in the experiment.\n"
           "--master-addr      Network address where the master server can be"
           "                   reached, in the form of <ip>:<port>"
           "                   (e.g., 10.10.1.2:5000).\n"
           "--log-file         Path to the file which is used for logging.\n"
           "\n"
           "After startup, the program will enter a loop reading lines from\n"
           "standard input and executing them as commands. The following\n"
           "commands are supported, each followed by a list of options\n"
           "supported by that command:\n\n"
           "tcp                Manage TCP connections within the cluster:\n"
           "  connect [port]   Establish all-to-all TCP connections between\n"
           "                   nodes in the cluster using a specific port\n"
           "                   to accept incoming connections."
           "  verify           Verify if every node is connected to every\n"
           "                   other nodes in the cluster.\n"
           "  disconnect       Tear down all TCP connections.\n"
           "\n"
           "gen_workload       Generate a shuffle workload as determined by\n"
           "                   the options.\n"
           "  --seed           Seed value used to generate the message sizes.\n"
           "  --avg-msg-size   Average length of the shuffle messages.\n"
           "  --skew-factor    Message skew factor (TODO: XXX).\n"
           "\n"
           "time_sync          Synchronize the clocks in the cluster.\n"
           "  --port           UDP port number dedicated to time_sync probes.\n"
           "  --seconds        Duration to run the time sync protocol.\n"
           "\n"
           "run_bench          Start running the shuffle benchmark.\n"
           "  --protocol       Transport protocol to use: homa or tcp\n"
           "  --epoll          Use epoll for efficient monitoring of incoming\n"
           "                   data at TCP sockets.\n"
           "  --policy         hadoop, lockstep, or LRPT\n"
           "  --max-unacked    Maximum number of outbound messages which can\n"
           "                   be in progress at any time.\n"
           "  --max-seg        Maximum number of bytes in a message segment.\n"
           "  --times          Number of times to repeat the experiment.\n"
           "\n"
           "log [msg]          Print all of the words that follow the command\n"
           "                   as a message to the log.\n"
           "\n"
           "exit               Exit the application.\n",
           exec_cmd
           );
    // TODO: should I give up homa in this project and use udp instead? the "thread_local" problem in shenango is really annoying when trying to do anything non-trivial
    // TODO: implement --epoll properly
    // FIXME: add command "tt" for timetrace
    // FIXME: how to simulate the senario of 10000 sockets? should I implement it in another program?
    // TODO: get memory intensive background traffic workload up and running (can we add an option like --add-interference)
}

/**
 * Parse the arguments of a "run_bench" command and execute it.
 *
 * \param words
 *      Command arguments (including the command name as @words[0]).
 * \return
 *      True means success, false means there was an error.
 */
bool
run_bench_cmd(std::vector<std::string>& words, shuffle_op& op)
{
    // fixme: move this method to run_bench.h?

    RunBenchOptions opts;
    if (!opts.parse_args(words)) {
        return false;
    }

    // FIXME: udp shuffle not implemented.
    if (!opts.tcp_protocol)
        return false;

    char ctrl_msg[32] = {};
    bool is_master = (cluster->local_rank == 0);
    for (size_t run = 0; run < opts.times; run++) {
        // Reset the shuffle_op object.
        op.in_msgs.clear();
        op.in_msgs.resize(cluster->num_nodes);
        op.next_inmsg_addr = op.rx_data.get();
        op.acked_out_msgs.reset(nullptr);

        // The master node broadcasts while the followers block.
        uint64_t bcast_tsc = rdtsc();
        if (!is_master) {
            cluster->control_socks[0]->ReadFull(ctrl_msg, 2);
            assert(strncmp(ctrl_msg, "GO", 2) == 0);
        } else {
            for (auto& c : cluster->control_socks) {
                c->WriteFull("GO", 2);
            }
        }

        // FIXME: the following piece of code seems awkward
        uint64_t elapsed_tsc = rdtsc();
        bool success = tcp_shuffle(opts, *cluster, op);
        if (!success) {
            return false;
        }
        elapsed_tsc = rdtsc() - elapsed_tsc;
        double elapsed_us = elapsed_tsc * 1.0 / cycles_per_us;

        double rx_speed = op.total_rx_bytes / (125.0 * elapsed_us);
        double tx_speed = op.total_tx_bytes / (125.0 * elapsed_us);

        // The master node blocks until all followers complete.
        if (!is_master) {
            cluster->control_socks[0]->WriteFull("DONE", 4);
            log_info("node-%d completed shuffle op %lu in %.1f us "
                     "(%.2f/%.2f Gbps)", cluster->local_rank, run, elapsed_us,
                    rx_speed, tx_speed);
        } else {
            for (auto& c : cluster->control_socks) {
                c->ReadFull(ctrl_msg, 4);
                assert(strncmp(ctrl_msg, "DONE", 4) == 0);
            }
            uint64_t bench_overhead = rdtsc() - bcast_tsc - elapsed_tsc;
            log_info("node-%d completed shuffle op %lu in %.1f us "
                     "(%.2f/%.2f Gbps), benchmark overhead %.1f us",
                     cluster->local_rank, run, elapsed_us, rx_speed, tx_speed,
                    bench_overhead * 1.0 / cycles_per_us);
        }
    }
    return true;
}

/**
 * Parse the arguments of a "time_sync" command and execute it.
 *
 * \param words
 *      Command arguments (including the command name as @words[0]).
 * \return
 *      True means success, false means there was an error.
 */
bool
time_sync_cmd(std::vector<std::string>& words)
{
    TimeSyncOptions opts;
    opts.parse_args(words);
    // fixme: move to time_sync.h
    return true;
}

/**
 * Parse the arguments of a "log" command and execute it.
 *
 * \param words
 *      Command arguments (including the command name as @words[0]).
 * \return
 *      True means success, false means there was an error.
 */
bool
log_cmd(std::vector<std::string>& words)
{
    assert(words[0] == "log");
    for (size_t i = 1; i < words.size(); i++) {
        const char* option = words[i].c_str();
        if (strncmp(option, "--", 2) != 0) {
            std::string message;
            for (size_t j = i; j < words.size(); j++) {
                if (j != i)
                    message.append(" ");
                message.append(words[j]);
            }
            log_info("%s", message.c_str());
            return true;
        }
    }
    return true;
}

/**
 * Given a command that has been parsed into words, execute the command
 * corresponding to the words.
 *
 * \param words
 *      Each entry represents one word of the command, like argc/argv.
 * \return
 *      True means success, false means there was an error.
 */
bool
exec_words(std::vector<std::string>& words)
{
	if (words.empty())
		return true;
	if (words[0] == "tcp") {
        return tcp_cmd(words, *cluster);
    } else if (words[0] == "gen_workload") {
		return gen_workload_cmd(words, *cluster, *current_op);
	} else if (words[0] == "time_sync") {
        return time_sync_cmd(words);
	} else if (words[0] == "run_bench") {
		return run_bench_cmd(words, *current_op);
	} else if (words[0] == "exit") {
		if (cmd_line_opts->log_file != stdout)
			log_info("shuffle_node exiting (exit command)");
		exit(0);
	} else if (words[0] == "log") {
        return log_cmd(words);
	} else {
		printf("Unknown command '%s'", words[0].c_str());
		return false;
	}
}

/**
 * Given a string, parse it into words and execute the resulting command.
 *
 * \param cmd
 *      Command to execute.
 * \return
 *      True means success, false means there was an error.
 */
bool
exec_string(const char* cmd)
{
	const char *p = cmd;
	std::vector<std::string> words;

	if (cmd_line_opts->log_file != stdout)
		log_info("Command: %s", cmd);

	while (true) {
		int word_length = strcspn(p, " \t\n");
		if (word_length > 0)
			words.emplace_back(p, word_length);
		p += word_length;
		if (*p == 0)
			break;
		p++;
	}
    return exec_words(words);
}

void
real_main(void* arg) {
    // Initialize static variables.
    Cluster cls;
    shuffle_op op;
    cluster = &cls;
    current_op = &op;

    cluster->init(cmd_line_opts);

    // Read commands from stdin and execute them.
    std::string line;
    while (true) {
        printf("%% ");
        fflush(stdout);
        if (!std::getline(std::cin, line)) {
            if (cmd_line_opts->log_file != stdout)
                log_info("cp_node exiting (EOF on stdin)");
            return;
        }

        bool success = exec_string(line.c_str());
        if (!success)
            log_err("failed to execute command '%s'", line.c_str());
    }
}

int main(int argc, char* argv[]) {
	if (((argc >= 2) && (strcmp(argv[1], "--help") == 0)) || (argc == 1)) {
		print_help(argv[0]);
		return 0;
	}

	CommandLineOptions opts;
    cmd_line_opts = &opts;
    opts.parse_args(argc-2, &argv[2]);
    log_init(opts.log_file);

    int ret = runtime_init(argv[1], real_main, nullptr);
    if (ret) {
        panic("failed to start Caladan runtime");
    }

    return 0;
}
