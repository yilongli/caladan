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

# This file contains library functions used to run cluster performance
# tests for the Linux kernel implementation of Homa.

import argparse
import copy
import datetime
import glob
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import traceback
import fcntl

# If a server's id appears as a key in this dictionary, it means we
# have started cp_node running on that node. The value of each entry is
# a Popen object that can be used to communicate with the node.
# active_nodes = {}
active_nodes = []

# The range of nodes currently running cp_node servers.
server_nodes = range(0,0)

# Directory containing log files.
log_dir = ''

# Local log file where each node writes its log messages.
log_file = ''

# Open file (in the log directory) where log messages should be written.
cperf_log_file = 0

# Indicates whether we should generate additional log messages for debugging
verbose = False

# Time when this benchmark was run.
date_time = str(datetime.datetime.now())

# Defaults for command-line options, if the application doesn't specify its
# own values.
default_defaults = {
    'server_list':         '/shome/rc-hosts.txt',
    'shuffle_dir':         '/shome/caladan/apps/shuffle',
    'shuffle_bin':         'shuffle_node',
    'caladan_cfg':         '~/caladan.config',
    'ifname':              'eno1d1',
    'server_port':         5000,
    'master_addr':         '10.10.1.2:5000',
    'log_dir':             'logs/' + time.strftime('%Y%m%d%H%M%S'),
    'log_file':            'node.log',
}

def get_parser(description, usage, defaults = {}):
    """
    Returns an ArgumentParser for options that are commonly used in
    performance tests.
    description:    A string describing the overall functionality of this
                    particular performance test
    usage:          A command synopsis (passed as usage to ArgumentParser)
    defaults:       A dictionary whose keys are option names and whose values
                    are defaults; used to modify the defaults for some of the
                    options (there is a default default for each option).
    """
    for key in default_defaults:
        if not key in defaults:
            defaults[key] = default_defaults[key]
    parser = argparse.ArgumentParser(description=description + ' The options '
            'below may include some that are not used by this particular '
            'benchmark', usage=usage, add_help=False,
            conflict_handler='resolve')
    parser.add_argument('--server-list', dest='server_list',
            default=defaults['server_list'],
            help='Path to the config file which contains all the nodes '
                 'available in the cluster (default: %s)'
            % (defaults['server_list']))
    parser.add_argument('--shuffle-dir', dest='shuffle_dir',
            default=defaults['shuffle_dir'],
            help='Top-level directory of the shuffle application (default: %s)'
            % (defaults['shuffle_dir']))
    parser.add_argument('--shuffle-bin', dest='shuffle_bin',
            default=defaults['shuffle_bin'],
            help='Name of the executable file (default: %s)'
            % (defaults['shuffle_bin']))
    parser.add_argument('--caladan-cfg', dest='caladan_cfg',
            default=defaults['caladan_cfg'],
            help='Path to the config file for Caladan runtime (default: %s)'
            % (defaults['caladan_cfg']))
    parser.add_argument('--ifname', dest='ifname', default=defaults['ifname'],
            help='Symbolic name of the network interface to use (default: %s)'
            % (defaults['ifname']))
    parser.add_argument('--server-port', type=int, dest='server_port',
            default=defaults['server_port'],
            help='Server port number used by the application (default: %d)'
            % (defaults['server_port']))
    parser.add_argument('--num-nodes', type=int, dest='num_nodes',
            help='Number of nodes used in the experiment')
    parser.add_argument('--master-addr', dest='master_addr',
            default=defaults['master_addr'],
            help='Network address of the master node (default: %s)'
            % (defaults['master_addr']))
    parser.add_argument('--log-dir', dest='log_dir',
            default=defaults['log_dir'],
            help='rcnfs directory used to collect log files from other nodes '
                 'in the cluster at the end of the experiment (default: %s)'
            % (defaults['log_dir']))
    parser.add_argument('--log-file', dest='log_file',
            default=defaults['log_file'],
            help='Local file used to log messages by each node (default: %s)'
            % (defaults['log_file']))
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
            help='Enable verbose output in node logs')
    return parser


def init(options):
    """
    Initialize various global state, such as the log file.
    """
    global log_dir, log_file, cperf_log_file, verbose
    log_dir = options.log_dir
    log_file = options.log_file

    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)
    os.makedirs(log_dir)
    os.symlink(log_dir, log_dir + "../latest", target_is_directory=True)

    cperf_log_file = open("%s/cperf.log" % (log_dir), "a")
    verbose = options.verbose
    vlog("cperf starting at %s" % (date_time))
    s = ""
    opts = vars(options)
    for name in sorted(opts.keys()):
        if len(s) != 0:
            s += ", "
        s += ("--%s: %s" % (name, str(opts[name])))
    vlog("Options: %s" % (s))


def log(message):
    """
    Write the a log message both to stdout and to the cperf log file.

    message:  The log message to write; a newline will be appended.
    """
    global cperf_log_file
    print(message)
    cperf_log_file.write(message)
    cperf_log_file.write("\n")


def vlog(message):
    """
    Log a message, like log, but if verbose logging isn't enabled, then
    log only to the cperf log file, not to stdout.

    message:  The log message to write; a newline will be appended.
    """
    global cperf_log_file, verbose
    if verbose:
        print(message)
    cperf_log_file.write(message)
    cperf_log_file.write("\n")


def wait_output(string, nodes, cmd, time_limit=10.0):
    """
    This method waits until a particular string has appeared on the stdout of
    each of the nodes in the list given by nodes. If a long time goes by without
    the string appearing, an exception is thrown.
    string:      The value to wait for
    cmd:         Used in error messages to indicate the command that failed
    time_limit:  An error will be generated if this much time goes by without
                 the desired string appearing
    """
    global active_nodes
    outputs = []
    printed = False

    for id in nodes:
        while len(outputs) <= id:
            outputs.append("")
    start_time = time.time()
    while time.time() < (start_time + time_limit):
        for id in nodes:
            data = active_nodes[id].stdout.read(1000)
            if data != None:
                print_data = data
                if print_data.endswith(string):
                    print_data = print_data[:(len(data) - len(string))]
                if print_data != "":
                    log("output from node-%d: '%s'" % (id, print_data))
                outputs[id] += data
        bad_node = -1
        for id in nodes:
            if not string in outputs[id]:
                bad_node = id
                break
        if bad_node < 0:
            return
        if (time.time() > (start_time + time_limit)) and not printed:
            log("expected output from node-%d not yet received "
            "after command '%s': expecting '%s', got '%s'"
            % (bad_node, cmd, string, outputs[bad_node]))
            printed = True;
        time.sleep(0.1)
    raise Exception("bad output from node-%d after command '%s': "
            "expected '%s', got '%s'"
            % (bad_node, cmd, string, outputs[bad_node]))


def get_cluster_nodes(cluster_config):
    """
    Read the cluster config file to obtain the list of the nodes in the cluster.

    :param cluster_config:
        Each line of the file contains a host name, its public IP address, and
        its private IP address, separated by whitespaces.
    :return:
        A list of nodes in the cluster; each node is represented by its hostname
        and public IP address.
    """
    cluster_nodes = []
    for line in open(cluster_config):
        try:
            hostname, public_ip, private_ip = line.strip().split(" ")
            cluster_nodes.append((hostname, public_ip))
        except:
            print("failed to parse '%s'" % line)
    return cluster_nodes


def cluster_exec(servers, cmd):
    """
    Execute the same command on a group of servers.

    :param servers:
        IP addresses of the servers, in text format.
    :param command:
        Command to run.
    """
    for hostname, ip in servers:
        vlog("Executing '%s' on %s" % (cmd, hostname))
        subprocess.Popen(["ssh", "-o", "StrictHostKeyChecking=no", ip, cmd])
        active_nodes.append((hostname, ip))


def stop_nodes():
    """
    Exit all of the nodes that are currently active.
    """
    # global active_nodes, server_nodes
    # for id, popen in homa_prios.items():
    #     subprocess.run(["ssh", "-o", "StrictHostKeyChecking=no",
    #             "node-%d" % id, "sudo", "pkill", "homa_prio"])
    #     try:
    #         popen.wait(5.0)
    #     except subprocess.TimeoutExpired:
    #         log("Timeout killing homa_prio on node-%d" % (id))
    # for node in active_nodes.values():
    #     node.stdin.write("exit\n")
    #     try:
    #         node.stdin.flush()
    #     except BrokenPipeError:
    #         log("Broken pipe to node-%d" % (id))
    # for node in active_nodes.values():
    #     node.wait(5.0)
    for hostname, ip in active_nodes:
        subprocess.run(["rsync", "-rtvq", "%s:%s" % (ip, log_file),
                "%s/%s.log" % (log_dir, hostname)])
    active_nodes.clear()
    # server_nodes = range(0,0)
