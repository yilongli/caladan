/*
 * timetrace.h - the timetrace service
 */

#pragma once

#include <stdlib.h>
#include <stdio.h>

#include <base/limits.h>
#include <base/time.h>

// Change 1 -> 0 in the following line to disable time tracing globally.
// Used only in debugging.
#define ENABLE_TIME_TRACE 1

/**
 * Timetrace implements a circular buffer of entries, each of which
 * consists of a fine-grain timestamp, a short descriptive string, and
 * a few additional values. It's typically used to record times at
 * various points in the runtime and iokernel, in order to find performance
 * bottlenecks. It can record a trace relatively efficiently (< 10ns as
 * of 6/2018), and the trace can be dumped to a file for analysis.
 */

/**
 * tt_event - holds one entry in a tt_buffer.
 */
struct tt_event {
	/**
	 * Time when this event occurred (in rdtsc units).
	 */
	uint64_t timestamp;
	/**
	 * Format string describing the event. NULL means that this
	 * entry has never been occupied.
	 */
	const char* format;

	/**
	 * Up to 4 additional arguments that may be referenced by
	 * @format when printing out this event.
	 */
	uint32_t arg0;
	uint32_t arg1;
	uint32_t arg2;
	uint32_t arg3;
};

/* The number of events in a tt_buffer, as a power of 2. */
#define TT_BUF_SIZE_EXP 14ul
#define TT_BUF_SIZE (1ul<<TT_BUF_SIZE_EXP)

/**
 * tt_buffer - Represents a sequence of events, typically consisting of all
 * those generated on one CPU.  Has a fixed capacity, so slots are re-used
 * on a circular basis.  This class is not thread-safe.
 */
struct tt_buffer {
	/* Index within events of the slot to use for the next tt_record call. */
	size_t next_index;

	/* True means a new event is currently being added to the buffer. */
	bool changing;

	/* Identifier of the CPU this buffer belongs to. */
	unsigned int cpu_id;

	/**
	 * Holds information from the most recent calls to tt_record.
	 * Updated circularly, so each new event replaces the oldest
	 * existing event.
	 */
	struct tt_event events[TT_BUF_SIZE];
};

extern int    tt_init();
extern void   tt_freeze(void);
extern int    tt_dump(FILE *output);
extern void   tt_record_buf(struct tt_buffer* buffer, uint64_t timestamp,
		const char* format, uint32_t arg0, uint32_t arg1,
		uint32_t arg2, uint32_t arg3);

/* exposed here so tt_recordN() can be inlined */
extern struct tt_buffer *tt_buffers[NCPU];

/**
 * tt_recordN(): record an event, along with N parameters.
 *
 * Note: this method accesses thread-local data; when used inside app/runtime,
 * preemption must be disabled during the call.
 *
 * @cpu_id:    Identifier of the current CPU.
 * @format:    Format string for snprintf that will be used, along with
 *             arg0..arg3, to generate a human-readable message describing
 *             what happened, when the time trace is printed. The message
 *             is generated by calling snprintf as follows:
 *                 snprintf(buffer, size, format, arg0, arg1, arg2, arg3)
 *             where format and arg0..arg3 are the corresponding arguments
 *             to this method. This pointer is stored in the buffer, so
 *             the caller must ensure that its contents will not change
 *             over its lifetime in the trace.
 * @arg0       Argument to use when printing a message about this event.
 * @arg1       Argument to use when printing a message about this event.
 * @arg2       Argument to use when printing a message about this event.
 * @arg3       Argument to use when printing a message about this event.
 */
static inline uint64_t tt_record4(unsigned cpu_id, const char* format,
        uint32_t arg0, uint32_t arg1, uint32_t arg2, uint32_t arg3)
{
    uint64_t now = 0;
#if ENABLE_TIME_TRACE
    now = rdtsc();
	tt_record_buf(tt_buffers[cpu_id], now, format, arg0, arg1, arg2, arg3);
#endif
	return now;
}
static inline uint64_t tt_record4_tsc(unsigned cpu_id, uint64_t tsc,
        const char* format, uint32_t arg0, uint32_t arg1, uint32_t arg2,
        uint32_t arg3)
{
#if ENABLE_TIME_TRACE
	tt_record_buf(tt_buffers[cpu_id], tsc, format, arg0, arg1, arg2, arg3);
#endif
	return tsc;
}
static inline uint64_t tt_record3(unsigned cpu_id, const char* format,
        uint32_t arg0, uint32_t arg1, uint32_t arg2)
{
    return tt_record4(cpu_id, format, arg0, arg1, arg2, 0);
}
static inline uint64_t tt_record2(unsigned cpu_id, const char* format,
        uint32_t arg0, uint32_t arg1)
{
    return tt_record4(cpu_id, format, arg0, arg1, 0, 0);
}
static inline uint64_t tt_record1(unsigned cpu_id, const char* format,
        uint32_t arg0)
{
    return tt_record4(cpu_id, format, arg0, 0, 0, 0);
}
static inline uint64_t tt_record0(unsigned cpu_id, const char* format)
{
    return tt_record4(cpu_id, format, 0, 0, 0, 0);
}
