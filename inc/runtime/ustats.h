/*
 * ustats.h - support for user-thread statistics
 */

#pragma once

#include <base/stddef.h>
#include <runtime/cpu.h>

/*
 * Routines for managing per-uthread statistics
 */


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

/*
 * Routines for managing per-cpu statistics
 */

#define DEFINE_PERCPU_METRIC(x) \
    uint64_t _percpu_metric_##x[NCPU]

#define percpu_metric_get(x) _percpu_metric_##x[cpu_get_current()]
#define percpu_metric_get_remote(x, cpu) _percpu_metric_##x[cpu]

/*
struct percpu_metric {
    uint64_t cpu[NCPU];
    const char *name;
};

#define DEFINE_PERCPU_METRIC2(x) \
    struct percpu_metric _percpu_metric_##x = { .name = #x };
*/