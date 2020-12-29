/*
 * stat.c - support for per-uthread statistics and counters
 */

#include <runtime/thread.h>
#include "defs.h"

/**
 * ustats_get_counter - reads a stat counter of the current thread
 * @id: identifier of the stat counter
 */
uint64_t ustats_get_counter(int id)
{
#ifdef RUNTIME_STATS
    return thread_self()->stats[id];
#else
    return 0;
#endif
}
