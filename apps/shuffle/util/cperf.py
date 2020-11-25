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
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
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
active_nodes = {}

# The range of nodes currently running cp_node servers.
server_nodes = range(0,0)

# Directory containing log files.
log_dir = ''

# Open file (in the log directory) where log messages should be written.
log_file = 0

# Indicates whether we should generate additional log messages for debugging
verbose = False

def log(message):
    """
    Write the a log message both to stdout and to the cperf log file.

    message:  The log message to write; a newline will be appended.
    """
    global log_file
    print(message)
    log_file.write(message)
    log_file.write("\n")


def vlog(message):
    """
    Log a message, like log, but if verbose logging isn't enabled, then
    log only to the cperf log file, not to stdout.

    message:  The log message to write; a newline will be appended.
    """
    global log_file, verbose
    if verbose:
        print(message)
    log_file.write(message)
    log_file.write("\n")


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
        Each line of the file contains a host name and an IP address, separated
        by a whitespace.
    :return:
        A dictionary from host names to IP addresses.
    """
    cluster_nodes = {}
    for line in open(cluster_config):
        hostname, ip_addr = line.strip().split(" ")
        cluster_nodes[hostname] = ip_addr
    return cluster_nodes


def cluster_exec(servers, cmd):
    """
    Execute the same command on a group of servers.

    :param servers:
        IP addresses of the servers, in text format.
    :param command:
        Command to run.
    """
    for server in servers:
        vlog("Executing '%s' on %s" % (cmd, server))
        subprocess.Popen(["ssh", "-o", "StrictHostKeyChecking=no", server, cmd])