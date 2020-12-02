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

/// Command-line options.
static CommandLineOptions cmd_line_opts;

/// Cluster of nodes used in the experiment.
static Cluster cluster;

/// Current shuffle workload.
static shuffle_op current_op;

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
           "--log-file         Path to the file which is used for logging.\n"
           "\n"
           "After startup, the program will enter a loop reading lines from\n"
           "standard input and executing them as commands. The following\n"
           "commands are supported, each followed by a list of options\n"
           "supported by that command:\n\n"
           "verify_conn        Check all-to-all connectivity in the cluster\n"
           "                   by performing an all-reduce on the node ranks.\n"
           "\n"
           "setup_workload     Configure the shuffle workload.\n"
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
           "  --policy         random, round-robin, or \"high-entropy\"\n"
           "  --max-unacked    Maximum number of outbound messages which can\n"
           "                   be in progress at any time.\n"
           "  --seg-size       Maximum bumber of bytes in message segment.\n"
           "  --times          Number of times to repeat the experiment.\n"
           "\n"
           "log [msg]          Print all of the words that follow the command\n"
           "                   as a message to the log.\n"
           "\n"
           "exit               Exits the application.\n",
           exec_cmd
           );
    // FIXME: how to simulate the senario of 10000 sockets? should I implement it in another program?
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
run_bench_cmd(rt::vector<rt::string>& words)
{
    RunBenchOptions opts;
    opts.parse_args(words);

    // fixme: move to run_bench.h?
//    tcp_shuffle(c, op);
//    tcp_epoll_shuffle(c);
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
time_sync_cmd(rt::vector<rt::string>& words)
{
    TimeSyncOptions opts;
    opts.parse_args(words);
    // fixme: move to time_sync.h
    return true;
}

/**
 * Parse the arguments of a "verify_conn" command and execute it.
 *
 * \return
 *      True means success, false means there was an error.
 */
bool
verify_conn_cmd()
{
    // Test all-to-all communication
    int sum = cluster.local_rank;
    for (auto& tcp_sock : cluster.tcp_socks) {
        if (!tcp_sock) continue;
        tcp_sock->WriteFull(&cluster.local_rank, sizeof(cluster.local_rank));
    }
    for (auto& tcp_sock : cluster.tcp_socks) {
        if (tcp_sock) {
            int r;
            tcp_sock->ReadFull(&r, sizeof(r));
            sum += r;
        }
    }
    int expected = (cluster.num_nodes - 1) * cluster.num_nodes / 2;
    if (sum == expected) {
        log_info("verify_conn: success");
        return true;
    } else {
        log_err("verify_conn: unexpected sum of ranks %d", sum);
        return false;
    }
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
log_cmd(rt::vector<rt::string>& words)
{
    assert(words[0] == "log");
    for (size_t i = 1; i < words.size(); i++) {
        const char* option = words[i].c_str();
        if (strncmp(option, "--", 2) != 0) {
            rt::string message;
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
exec_words(rt::vector<rt::string>& words)
{
	if (words.empty())
		return true;
	if (words[0] == "verify_conn") {
        return verify_conn_cmd();
    } else if (words[0] == "setup_workload") {
		return setup_workload_cmd(words, cluster, current_op);
	} else if (words[0] == "time_sync") {
        return time_sync_cmd(words);
	} else if (words[0] == "run_bench") {
		return run_bench_cmd(words);
	} else if (words[0] == "exit") {
		if (cmd_line_opts.log_file != stdout)
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
	rt::vector<rt::string> words;

	if (cmd_line_opts.log_file != stdout)
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
    cluster.init(&cmd_line_opts);
    cluster.connect_all();

    // Read commands from stdin and execute them.
    rt::string line;
    while (true) {
        printf("%% ");
        fflush(stdout);
        if (!std::getline(std::cin, line)) {
            if (cmd_line_opts.log_file != stdout)
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

    cmd_line_opts.parse_args(argc-2, &argv[2]);
    log_init(cmd_line_opts.log_file);
    int ret = runtime_init(argv[1], real_main, nullptr);
    if (ret) {
        panic("failed to start Caladan runtime");
    }

    return 0;
}
