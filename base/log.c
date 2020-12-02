/*
 * log.c - the logging system
 */

#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <execinfo.h>
#include <sched.h>

#include <base/stddef.h>
#include <base/log.h>
#include <base/time.h>
#include <asm/ops.h>

#define MAX_LOG_LEN 4096

/* log levels greater than this value won't be printed */
int max_loglevel = LOG_DEBUG;
/* stored here to avoid pushing too much on the stack */
static __thread char buf[MAX_LOG_LEN];
/* file where log messages should be written */
static FILE* log_file;

void logk(int level, const char *fmt, ...)
{
	va_list ptr;
	off_t off;
	int cpu;

	if (level > max_loglevel)
		return;

	cpu = sched_getcpu();

	if (likely(base_init_done)) {
		uint64_t us = microtime();
		sprintf(buf, "[%3d.%06d] CPU %02d| <%d> ",
			(int)(us / ONE_SECOND), (int)(us % ONE_SECOND),
			cpu, level);
	} else {
		sprintf(buf, "CPU %02d| <%d> ", cpu, level);
	}

	off = strlen(buf);
	va_start(ptr, fmt);
	int n = vsnprintf(buf + off, MAX_LOG_LEN - off, fmt, ptr);
	va_end(ptr);
	off_t end = MIN(off + n, MAX_LOG_LEN - 2);
	buf[end] = '\n';
	buf[end + 1] = 0;
	FILE* stream = log_file ? log_file : stdout;
	fputs(buf, stream);

	if (level <= LOG_ERR)
		fflush(stream);
}

#define MAX_CALL_DEPTH	256
void logk_backtrace(void)
{
	void *buf[MAX_CALL_DEPTH];
	const int calls = backtrace(buf, ARRAY_SIZE(buf));
	backtrace_symbols_fd(buf, calls, 1);
}

void logk_bug(bool fatal, const char *expr,
	      const char *file, int line, const char *func)
{
	logk(LOG_EMERG, "%s: %s:%d ASSERTION '%s' FAILED IN '%s'",
	     fatal ? "FATAL" : "WARN", file, line, expr, func);
	logk_backtrace();

	if (fatal)
		init_shutdown(EXIT_FAILURE);
}

/**
 * log_init - sets the stream where the messages should be written;
 * this method should be called only once (best before @runtime_init)
 * @file: the opened log file or NULL if stdout should be used for logging
 */
void log_init(FILE* file)
{
    if (log_file)
        log_err("log_file can only be set once");
    else
        log_file = file;
}
