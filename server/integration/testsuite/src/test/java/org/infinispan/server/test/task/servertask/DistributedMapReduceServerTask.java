package org.infinispan.server.test.task.servertask;

import org.infinispan.tasks.TaskExecutionMode;

/**
 * MapReduce task for executing on cluster.
 *
 * @author amanukya
 */
public class DistributedMapReduceServerTask extends LocalMapReduceServerTask {
    public static final String CACHE_NAME = "customTaskRepl";
    public static final String NAME = "dist_mapreduce_task";

    @Override
    public TaskExecutionMode getExecutionMode() {
        return TaskExecutionMode.ALL_NODES;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
