#!/usr/bin/python3

# Copyright (c) 2019-2020 Stanford University
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""
Scan a collection of unordered time trace events in a log file; rearrange
the events based on timestamps and output them with correct delta. If the
file is omitted, standard input is used.
Usage: ttmerge.py [clock_khz] [file]
"""

from __future__ import division, print_function
import re
import sys

def scan(f, clock_khz):
    '''
    Scan all timetrace events from an input source, sort them by timestamps,
    rewrite the deltas between events, and print them to stdout.
    :param f: input source to read timetrace events
    :param clock_khz: cpu frequency, in khz
    '''
    events = []
    p = re.compile('([0-9]+) *\| *[0-9.]+ us \(\+ *[0-9.]+ us\) (.*)')

    for line in f:
        match = p.match(line)
        if not match:
            continue

        timestamp = int(match.group(1))
        message = match.group(2)
        events.append((timestamp, message))

    events.sort()
    if len(events) == 0:
        return

    start_tsc, _ = events[0]
    prev_time = 0.0
    for timestamp, message in events:
        time = (timestamp - start_tsc) / clock_khz
        print("%d | %9.3f us (+%8.3f us) %s" % (timestamp, time,
                time - prev_time, message))
        prev_time = time


clock_khz = 2394.0
f = sys.stdin
if len(sys.argv) == 2:
    clock_khz = float(sys.argv[1])
elif len(sys.argv) == 3:
    clock_khz = float(sys.argv[1])
    f = open(sys.argv[2])
else:
    print("Usage: %s [clock_khz] [logFile]" % (sys.argv[0]))
    sys.exit(1)

scan(f, clock_khz)