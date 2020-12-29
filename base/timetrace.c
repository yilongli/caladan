/*
 * timetrace.c - support for the timetrace service
 */

#include <base/cpu.h>
#include <base/log.h>
#include <base/timetrace.h>

#include <sched.h>

/* Separate tt_buffers for each cpu.
 */
struct tt_buffer *tt_buffers[NCPU];

/* Zero means that tt_freeze has not been called since the last time the
 * timetrace was dumped; one means tt_freeze has been called but has not
 * completed; other values mean tt_freeze has completed successfully.
 */
static unsigned int tt_frozen;

/**
 * tt_init - creates core-private tt_buffer's for all cpus in the system.
 *
 * Return:    0 for success, else a negative errno.
 */
int tt_init()
{
    int i;
    struct tt_buffer *tt_buf;

    for (i = 0; i < cpu_count; i++) {
        tt_buf = aligned_alloc(CACHE_LINE_SIZE,
                align_up(sizeof(*tt_buf), CACHE_LINE_SIZE));
        if (!tt_buf)
            return -ENOMEM;

        tt_buf->cpu_id = i;
        tt_buffers[i] = tt_buf;
    }
    return 0;
}

/**
 * tt_freeze() - Stop recording timetrace events until the trace has been
 * dumped to a file. When recording resumes after dumping the trace, the
 * buffers will be cleared.
 */
void tt_freeze(void)
{
    int i;
    struct tt_buffer *buffer;

    if (load_acquire(&tt_frozen))
        return;
    tt_record0(sched_getcpu(), "timetrace frozen");
    store_release(&tt_frozen, 1);

    /* busy-spin until all concurrent tt_record_buf()'s have finished */
    for (i = 0; i < cpu_count; i++) {
        buffer = tt_buffers[i];
        while (load_acquire(&buffer->changing));
    }
    store_release(&tt_frozen, 2);
}

/**
 * tt_dump() - append all timetrace events to the end of a file. 
 * @output:   The file to write the timetrace events into.
 *
 * Return:    0 for success, else a negative errno.
 */
int tt_dump(FILE *output)
{
    struct tt_buffer* buffer;
    uint64_t start_time;
    int i;
    int result = 0;
    double prev_micros, micros;

    /* Index of the next entry to return from each tt_buffer. */
    int pos[NCPU];

    /* tt_freeze() must be called before */
    if (load_acquire(&tt_frozen) < 2) {
        return -EFAULT;
    }

    start_time = 0;
    for (i = 0; i < cpu_count; i++) {
        buffer = tt_buffers[i];
        if (buffer->events[TT_BUF_SIZE-1].format == NULL) {
            pos[i] = 0;
        } else {
            int index = (buffer->next_index + 1) & (TT_BUF_SIZE-1);
            pos[i] = index;
        }
        struct tt_event *event = &buffer->events[pos[i]];
        if (event->format && (event->timestamp > start_time)) {
            start_time = event->timestamp;
        }
    }

    /* Skip over all events before start_time, in order to make
     * sure that there's no missing data in what we print.
     */
    for (i = 0; i < cpu_count; i++) {
        buffer = tt_buffers[i];
        while ((buffer->events[pos[i]].timestamp < start_time)
                && (pos[i] != buffer->next_index)) {
            pos[i] = (pos[i] + 1) & (TT_BUF_SIZE-1);
        }
    }


    /* Each iteration through this loop processes one event (the one
     * with the earliest timestamp).
     */
    prev_micros = 0.0;
    while (1) {
        struct tt_event *event;
        int curr_buf = -1;
        uint64_t earliest_time = ~0lu;

        /* Check all the traces to find the earliest available event. */
        for (i = 0; i < cpu_count; i++) {
            buffer = tt_buffers[i];
            event = &buffer->events[pos[i]];
            if ((pos[i] != buffer->next_index)
                    && (event->timestamp < earliest_time)) {
                curr_buf = i;
                earliest_time = event->timestamp;
            }
        }
        if (curr_buf < 0) {
            /* None of the traces have any more events to process. */
            fflush(output);
            break;
        }

        /* Format one event. */
        event = &(tt_buffers[curr_buf]->events[pos[curr_buf]]);
        micros = (double) (event->timestamp - start_time) / cycles_per_us;

        fprintf(output, "%lu | %9.3f us (+%8.3f us) [CPU %02u] ",
                event->timestamp, micros, micros - prev_micros,
                tt_buffers[curr_buf]->cpu_id);
        fprintf(output, event->format, event->arg0, event->arg1, event->arg2,
                event->arg3);
        fputc('\n', output);
        prev_micros = micros;
        pos[curr_buf] = (pos[curr_buf] + 1) & (TT_BUF_SIZE - 1);
    }

    /* Reset tt_buffer's and resume recording */
    for (i = 0; i < cpu_count; i++) {
        buffer = tt_buffers[i];
        buffer->events[TT_BUF_SIZE-1].format = NULL;
        buffer->next_index = 0;
    }
    store_release(&tt_frozen, 0);

    return result;
}

/**
 * tt_record_buf(): record an event in a specific tt_buffer.
 *
 * @buffer:    Buffer in which to record the event.
 * @timestamp: The time at which the event occurred (rdtsc units)
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
void tt_record_buf(struct tt_buffer *buffer, uint64_t timestamp,
        const char* format, uint32_t arg0, uint32_t arg1, uint32_t arg2,
        uint32_t arg3)
{
    struct tt_event *event;
    if (load_acquire(&tt_frozen))
        return;

    store_release(&buffer->changing, 1);
    event = &buffer->events[buffer->next_index];
    buffer->next_index = (buffer->next_index + 1) & (TT_BUF_SIZE-1);

    event->timestamp = timestamp;
    event->format = format;
    event->arg0 = arg0;
    event->arg1 = arg1;
    event->arg2 = arg2;
    event->arg3 = arg3;
    store_release(&buffer->changing, 0);
}
