/*
 * cpu.c - support for accessing low-level cpu-related information
 */

#include "defs.h"

unsigned int cpu_get_current()
{
    return myk()->curr_cpu;
}