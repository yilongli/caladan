/*
 * timetrace.c - support for recording timetrace from the runtime.
 */

#include <runtime/timetrace.h>
#include "defs.h"

/**
 * __tt_record_np - disables preemption during the call to tt_record4().
 */
uint64_t __tt_record_np(const char* format, uint32_t arg0,
        uint32_t arg1, uint32_t arg2, uint32_t arg3)
{
    struct kthread *th;
    uint64_t tsc;

    /* disable preemption to prevent concurrent writes to the same tt_buffer:
     * this can happen when one uthread is migrated to another cpu during
     * tt_recordN(). */
    th = getk();
	tsc = tt_record4(th->curr_cpu, format, arg0, arg1, arg2, arg3);
	putk();
    return tsc;
}
