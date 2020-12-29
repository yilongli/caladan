/*
 * timetrace.h - support for timetrace
 */

#pragma once

#include <base/timetrace.h>
#include <runtime/preempt.h>

/*
 * internal implementation of tt_recordN_np(); unfortunately, it can't be
 * inlined as it requires method getk() inside runtime/defs.h
 */
extern uint64_t __tt_record_np(const char* format, uint32_t arg0,
        uint32_t arg1, uint32_t arg2, uint32_t arg3);

/**
 * tt_recordN_np - disables preemption when recording the timetrace message.
 */
static inline uint64_t tt_record4_np(const char* format, uint32_t arg0,
        uint32_t arg1, uint32_t arg2, uint32_t arg3)
{
#if ENABLE_TIME_TRACE
    return __tt_record_np(format, arg0, arg1, arg2, arg3);
#else
	return 0;
#endif
}

static inline uint64_t tt_record3_np(const char* format, uint32_t arg0,
        uint32_t arg1, uint32_t arg2)
{
    return tt_record4_np(format, arg0, arg1, arg2, 0);
}

static inline uint64_t tt_record2_np(const char* format, uint32_t arg0,
        uint32_t arg1)
{
    return tt_record4_np(format, arg0, arg1, 0, 0);
}

static inline uint64_t tt_record1_np(const char* format, uint32_t arg0)
{
    return tt_record4_np(format, arg0, 0, 0, 0);
}

static inline uint64_t tt_record_np(const char* format)
{
    return tt_record4_np(format, 0, 0, 0, 0);
}
