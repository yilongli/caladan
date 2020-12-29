/*
 * ustats.h - support for user-thread statistics
 */

#pragma once

#include <base/stddef.h>

/*
 * These are per-uthread stat counters. All counters are monotonically
 * increasing, as that decouples the counters from any particular collection
 * time period.
 */
enum {
	USTAT_UDP_BLOCK_CYCLES = 0,

	/* total number of counters */
	USTAT_NR,
};

extern uint64_t ustats_get_counter(int id);
