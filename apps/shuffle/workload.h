#pragma once

#include "cluster.h"
#include "options.h"
#include "shuffle_common.h"

bool setup_workload_cmd(rt::vector<rt::string> &words, Cluster &cluster,
        shuffle_op &op);
