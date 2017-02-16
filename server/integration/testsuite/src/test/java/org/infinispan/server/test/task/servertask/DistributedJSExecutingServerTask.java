package org.infinispan.server.test.task.servertask;

import org.infinispan.tasks.TaskExecutionMode;

/**
 * Server task for executing script over infinispan.
 *
 * @author amanukya
 */
public class DistributedJSExecutingServerTask extends JSExecutingServerTask {
    public static final String NAME = "dist_jsexecutor_task";
    public static final String CACHE_NAME = "customTaskRepl";

    @Override
    public TaskExecutionMode getExecutionMode() {
        return TaskExecutionMode.ALL_NODES;
    }

    @Override
    public String getName() {
        return NAME;
    }

    public String getCacheName() {
        return CACHE_NAME;
    }
}
