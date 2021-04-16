#!/usr/bin/python3

# Copyright (c) 2020 Stanford University
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

# This cperf benchmark computes shuffle performance as a function of the
# message size and message skewness using a fixed-size cluster.

from cperf import *

parser = get_parser(description='Shuffle throughput as a function of '
                                'the message size and message skewness.',
                    usage='%(prog)s [options]', defaults={})
parser.usage = parser.format_help()
options = parser.parse_args()

init(options)

# TODO:
message_sizes = []
# message_sizes += list(range(1000, 10000, 1000))
# message_sizes += list(range(10000, 50000, 5000))
message_sizes = [50000]

if not options.plot_only:
    cluster_size = options.num_nodes
    nodes = range(1, cluster_size + 1)
    command = "%s/%s %s --ifname %s --num-nodes %d --master-addr %s" \
              " --log-file %s" % \
              (options.shuffle_dir, options.shuffle_bin,
               options.caladan_cfg, options.ifname, cluster_size,
               options.master_addr, options.log_file)

    try:
        log("Starting nodes with command:\n%s" % command)
        start_nodes(nodes, command)

        run_bench_prefix = "run_bench --protocol udp --udp-port 5002 "

        for msg_size in message_sizes:
            for msg_skew in [1e-2 * x for x in range(300, 1000, 50)]:
            # for msg_skew in [var ** 0.5 for var in [0.1, 0.2, 0.5, 1, 5, 10, 100, 500, 10000]]:
                do_cmd(
                    "gen_workload --avg-msg-size %d --data-dist zipf %.2f --log"
                    % (msg_size, msg_skew), nodes)

                do_cmd("run_bench --protocol udp --udp-port 5002 --policy LRPT --times 20", nodes)
                # do_cmd("run_bench --protocol udp --udp-port 5002 --policy hadoop --max-unacked 5 --times 20", nodes)
                # do_cmd("run_bench --protocol udp --udp-port 5002 --policy hadoop --max-unacked 2 --times 20", nodes)
                # for policy in ["hadoop", "LRPT", "SRPT"]:
                #     do_cmd("run_bench --protocol udp --udp-port 5002 "
                #            "--policy %s --times 20" % policy, nodes)

        log("Stopping nodes")
        stop_nodes()
    except Exception as e:
        log("Caught exception:\n\t" + str(e))
        log("Cleaning up orphaned '%s' processes" % options.shuffle_bin)
        do_ssh(["pkill", options.shuffle_bin], nodes)

# Parse the log files to extract useful data
out = open(options.log_dir + "/cp_message_skewness.data", "w")
out.write("policy max_active nodes avg_msg_bytes msg_skew throughput_gbps\n")
for line in open(options.log_dir + "/rc01.log", "r"):
    match = re.match('.*collected ([0-9.]+).*'
                     'policy ([a-zA-Z]+).*'
                     'max active ([0-9]+).*'
                     'cluster size ([0-9.]+).*'
                     'avg. msg size ([0-9.]+).*'
                     'msg skewness ([0-9.]+).*'
                     'part. skewness ([0-9.]+).*'
                     'throughput ([0-9.]+)', line)
    if match:
        samples = int(match.group(1))
        policy = match.group(2)
        max_active = int(match.group(3))
        cluster_size = int(match.group(4))
        avg_msg_size = int(match.group(5))
        msg_skew = float(match.group(6))
        part_skew = float(match.group(7))
        throughput = float(match.group(8))
        out.write("%s %d %d %d %.2f %.1f\n" % (
            policy, max_active, cluster_size, avg_msg_size, msg_skew,
            throughput))
