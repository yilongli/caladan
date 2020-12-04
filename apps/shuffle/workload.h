#pragma once

#include "cluster.h"
#include "options.h"
#include "shuffle_common.h"

bool setup_workload_cmd(std::vector<std::string> &words, Cluster &cluster,
        shuffle_op &op);
