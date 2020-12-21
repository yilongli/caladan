/*
 * timetrace.c - support for the timetrace service
 */

#include <base/cpu.h>
#include <base/log.h>
#include <base/timetrace.h>

/* Separate tt_buffers for each kthread.
 */
struct tt_buffer *tt_buffers[NCPU];

/* actual number of tt_buffer's */
atomic_t nr_tt_buffers;

/* Kernel thread-local timetrace buffer. */
__thread struct tt_buffer *perthread_tt_buf;

/* Zero means that tt_freeze has not been called since the last time the
 * timetrace was dumped; one means tt_freeze has been called but has not
 * completed; other values mean tt_freeze has completed successfully.
 */
static unsigned int tt_frozen;

/**
 * tt_init_thread - creates a thread-private tt_buffer for the current thread.
 * @name:  Short descriptive name for the current thread; will appear in
 *         time trace printouts.
 *
 * Return the new tt_buffer object if successful, or NULL if out of memory.
 */
struct tt_buffer *tt_init_thread(char *name)
{
    struct tt_buffer *tt_buf;
    int tt_id;

    if (perthread_tt_buf)
        panic("thread-local tt_buffer already exists!");

    tt_buf = aligned_alloc(CACHE_LINE_SIZE,
			  align_up(sizeof(*tt_buf), CACHE_LINE_SIZE));
    if (!tt_buf)
        return NULL;
    perthread_tt_buf = tt_buf;

    tt_id = atomic_fetch_and_add(&nr_tt_buffers, 1);
    if (tt_id >= ARRAY_SIZE(tt_buffers))
        panic("tt_init_thread: too many kthreads!");
    tt_buffers[tt_id] = tt_buf;

    memset(tt_buf, 0, sizeof(*tt_buf));
    strncpy(tt_buf->name, name, TT_BUF_NAME_LEN-1);

    return tt_buf;
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
    tt_record("timetrace frozen");
    store_release(&tt_frozen, 1);

    /* busy-spin until all concurrent tt_record_buf()'s have finished */
    for (i = 0; i < atomic_read(&nr_tt_buffers); i++) {
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

    /* Messages are collected here, so they can be dumped out to
     * user space in bulk.
     */
#define TT_PF_BUF_SIZE 4000
    char msg_storage[TT_PF_BUF_SIZE];

    /* # bytes of data that have accumulated in msg_storage but haven't been
     * copied to user space yet. */
    int buffered;

    /* tt_freeze() must be called before */
    if (load_acquire(&tt_frozen) < 2) {
        return -EFAULT;
    }

    start_time = 0;
    for (i = 0; i < atomic_read(&nr_tt_buffers); i++) {
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
    for (i = 0; i < atomic_read(&nr_tt_buffers); i++) {
        buffer = tt_buffers[i];
        while ((buffer->events[pos[i]].timestamp < start_time)
                && (pos[i] != buffer->next_index)) {
            pos[i] = (pos[i] + 1) & (TT_BUF_SIZE-1);
        }
    }


    /* Each iteration through this loop processes one event (the one
     * with the earliest timestamp). We buffer data until msg_storage
     * is full, then copy to user space and repeat.
     */
    buffered = 0;
    prev_micros = 0.0;
    while (1) {
        struct tt_event *event;
        int entry_length, available;
        int curr_buf = -1;
        uint64_t earliest_time = ~0lu;

        /* Check all the traces to find the earliest available event. */
        for (i = 0; i < atomic_read(&nr_tt_buffers); i++) {
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
            goto flush;
        }

        /* Format one event. */
        event = &(tt_buffers[curr_buf]->events[pos[curr_buf]]);
        micros = (double) (event->timestamp - start_time) / cycles_per_us;
        available = TT_PF_BUF_SIZE - buffered;
        if (available == 0) {
            goto flush;
        }
        entry_length = snprintf(msg_storage + buffered, available,
                "%lu | %9.3f us (+%8.3f us) [%-6s] ", event->timestamp,
                micros, micros - prev_micros, tt_buffers[curr_buf]->name);
        prev_micros = micros;
        if (available >= entry_length)
            entry_length += snprintf(
                    msg_storage + buffered + entry_length,
                    available - entry_length,
                    event->format, event->arg0,
                    event->arg1, event->arg2, event->arg3);
        if (entry_length >= available) {
            /* Not enough room for this entry. */
            if (buffered == 0) {
                /* Even a full buffer isn't enough for
                 * this entry; truncate the entry. */
                entry_length = available - 1;
            } else {
                goto flush;
            }
        }
        /* Replace terminating null character with newline. */
        msg_storage[buffered + entry_length] = '\n';
        buffered += entry_length + 1;
        pos[curr_buf] = (pos[curr_buf] + 1) & (TT_BUF_SIZE - 1);
        continue;

        flush:
        if (fwrite(msg_storage, buffered, 1, output) < 1) {
            result = -EFAULT;
            goto done;
        }
        buffered = 0;
        if (curr_buf < 0)
            break;
    }

    /* Reset tt_buffer's and resume recording */
    for (i = 0; i < atomic_read(&nr_tt_buffers); i++) {
        buffer = tt_buffers[i];
        buffer->events[TT_BUF_SIZE-1].format = NULL;
        buffer->next_index = 0;
    }
    store_release(&tt_frozen, 0);

    done:
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
