/*
 * timetrace.h - support for recording time trace events from the runtime
 */

#pragma once

#include <base/timetrace.h>
#include <runtime/cpu.h>
#include <runtime/preempt.h>

/**
 * tt_recordN_np - disables preemption when recording the timetrace message.
 */
static inline void tt_record4_np(const char* format, uint32_t arg0,
        uint32_t arg1, uint32_t arg2, uint32_t arg3)
{
#if ENABLE_TIME_TRACE
    /* disable preemption to prevent concurrent writes to the same tt_buffer:
     * this can happen when one uthread is migrated to another cpu during
     * tt_recordN(). */
    preempt_disable();
	tt_record4(cpu_get_current(), format, arg0, arg1, arg2, arg3);
	preempt_enable();
#endif
}

static inline void tt_record3_np(const char* format, uint32_t arg0,
        uint32_t arg1, uint32_t arg2)
{
    tt_record4_np(format, arg0, arg1, arg2, 0);
}

static inline void tt_record2_np(const char* format, uint32_t arg0,
        uint32_t arg1)
{
    tt_record4_np(format, arg0, arg1, 0, 0);
}

static inline void tt_record1_np(const char* format, uint32_t arg0)
{
    tt_record4_np(format, arg0, 0, 0, 0);
}

static inline void tt_record_np(const char* format)
{
    tt_record4_np(format, 0, 0, 0, 0);
}
