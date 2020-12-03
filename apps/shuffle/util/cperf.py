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

# If a server's id appears as a key in this dictionary, it means we have
# started shuffle_node running on that node. The value of each entry is
# a Popen object that can be used to communicate with the node.
active_nodes = {}

# A dictionary whose keys are server ids and whose values are CloudLab public
# IP addresses which are used to establish ssh connections.
node_ssh_addrs = {}

# Directory containing log files.
log_dir = ''

# Local log file where each node writes its log messages.
log_file = ''

# Open file (in the log directory) where log messages should be written.
cperf_log = 0

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
    'master_addr':         '10.10.1.2:5000',
    'log_dir':             'logs/' + time.strftime('%Y%m%d%H%M%S'),
    'log_file':            'node.log',
}


def log(message):
    """
    Write the a log message both to stdout and to the cperf log file.

    message:  The log message to write; a newline will be appended.
    """
    global cperf_log
    print(message)
    cperf_log.write(message)
    cperf_log.write("\n")


def vlog(message):
    """
    Log a message, like log, but if verbose logging isn't enabled, then
    log only to the cperf log file, not to stdout.

    message:  The log message to write; a newline will be appended.
    """
    global cperf_log, verbose
    if verbose:
        print(message)
    cperf_log.write(message)
    cperf_log.write("\n")


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
    parser.add_argument('--plot-only', dest='plot_only', action='store_true',
            help='Don\'t run experiments; generate plot(s) with existing data')
    return parser


def init(options):
    """
    Initialize various global state, such as the log file.
    """
    global log_dir, log_file, cperf_log, verbose
    log_dir = options.log_dir
    log_file = options.log_file

    if not options.plot_only:
        if os.path.exists(log_dir):
            shutil.rmtree(log_dir)
        os.makedirs(log_dir)
        latest = log_dir + "/../latest"
        os.unlink(latest)
        os.symlink(os.path.abspath(log_dir), latest, target_is_directory=True)

    cperf_log = open("%s/cperf.log" % (log_dir), "a")
    verbose = options.verbose
    vlog("cperf starting at %s" % (date_time))
    s = ""
    opts = vars(options)
    for name in sorted(opts.keys()):
        if len(s) != 0:
            s += ", "
        s += ("--%s: %s" % (name, str(opts[name])))
    vlog("Options: %s" % (s))

    cluster_init(options.server_list)


def cluster_init(server_list):
    """
    Read the server list file to obtain the list of the nodes in the cluster.

    :param server_list:
        Each line of the file contains a host name, its public IP address, and
        its private IP address, separated by whitespaces.
    :return:
        A list of nodes in the cluster; each node is represented by its hostname
        and public IP address.
    """
    global node_ssh_addrs
    for line in open(server_list):
        try:
            hostname, public_ip = line.strip().split(" ")[:2]
            id = int(hostname[2:])
            node_ssh_addrs[id] = public_ip
        except:
            print("failed to parse '%s' in file %s" % (line, server_list))


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
            if data is not None:
                print_data = data
                if print_data.endswith(string):
                    print_data = print_data[:(len(data) - len(string))]
                if print_data != "":
                    log("output from rc%02d: '%s'" % (id, print_data))
                outputs[id] += data
        bad_node = -1
        for id in nodes:
            if not string in outputs[id]:
                bad_node = id
                break
        if bad_node < 0:
            return
        if (time.time() > (start_time + time_limit)) and not printed:
            log("expected output from rc%02d not yet received "
            "after command '%s': expecting '%s', got '%s'"
            % (bad_node, cmd, string, outputs[bad_node]))
            printed = True
        time.sleep(0.1)
    raise Exception("bad output from rc%02d after command '%s': "
            "expected '%s', got '%s'"
            % (bad_node, cmd, string, outputs[bad_node]))


def start_nodes(r, shell_cmd):
    """
    Start up shuffle_node on a group of nodes.
    r:          The range of nodes on which to start shuffle_node, if it isn't
                already running
    shell_cmd:  Shell command used to start shuffle_node
    """
    global active_nodes
    started = []
    for id in r:
        if id in active_nodes:
            continue
        vlog("Starting shuffle_node on rc%02d" % (id))
        node = subprocess.Popen(["ssh", "-o", "StrictHostKeyChecking=no",
                node_ssh_addrs[id], shell_cmd], encoding="utf-8",
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT)
        fl = fcntl.fcntl(node.stdin, fcntl.F_GETFL)
        fcntl.fcntl(node.stdin, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        fl = fcntl.fcntl(node.stdout, fcntl.F_GETFL)
        fcntl.fcntl(node.stdout, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        active_nodes[id] = node
        started.append(id)
    # FIXME: what is this? am I supposed to print something in shuffle_node.cc?
    wait_output("% ", started, "ssh")
    # FIXME: shall I implement "log" command in shuffle_node? maybe w/o --level option?
    # log_level = "normal"
    # if verbose:
    #     log_level = "verbose"
    # command = "log --file node.log --level %s" % (log_level)
    # for id in started:
    #     active_nodes[id].stdin.write(command + "\n")
    #     active_nodes[id].stdin.flush()
    # wait_output("% ", started, command)


def stop_nodes():
    """
    Exit all of the nodes that are currently active.
    """
    global active_nodes, node_ssh_addrs, log_file
    for id, node in active_nodes.items():
        node.stdin.write("exit\n")
        try:
            node.stdin.flush()
        except BrokenPipeError:
            log("Broken pipe to rc%02d" % id)
    for node in active_nodes.values():
        node.wait(5.0)
    for id in active_nodes:
        ip = node_ssh_addrs[id]
        subprocess.run(["rsync", "-rtvq", "%s:%s" % (ip, log_file),
                "%s/rc%02d.log" % (log_dir, id)])
    active_nodes.clear()


def do_cmd(command, r, r2 = range(0,0)):
    """
    Execute a shuffle_node command on a given group of nodes.
    command:    A command to execute on each node
    r:          A group of node ids on which to run the command (range, list,
                etc.)
    r2:         An optional additional group of node ids on which to run the
                command; if a note is present in both r and r2, the
                command will only be performed once
    """
    global active_nodes
    nodes = []
    for id in r:
        nodes.append(id)
    for id in r2:
        if id not in r:
            nodes.append(id)
    for id in nodes:
        vlog("Command for rc%02d: %s" % (id, command))
        active_nodes[id].stdin.write(command + "\n")
        try:
            active_nodes[id].stdin.flush()
        except BrokenPipeError:
            log("Broken pipe to rc%02d" % (id))
    wait_output("% ", nodes, command)


def do_ssh(command, nodes):
    """
    Use ssh to execute a particular shell command on a group of nodes.
    command:  command to execute on each node (a list of argument words)
    nodes:    specifies ids of the nodes on which to execute the command:
              should be a range, list, or other object that supports "in"
    """
    global node_ssh_addrs
    vlog("ssh command on nodes %s: %s" % (str(nodes), " ".join(command)))
    for id in nodes:
        subprocess.run(["ssh", node_ssh_addrs[id]] + command,
                stdout=subprocess.DEVNULL)


# FIXME: run_experiments, scan_log, etc. are dropped for now; do I need them?