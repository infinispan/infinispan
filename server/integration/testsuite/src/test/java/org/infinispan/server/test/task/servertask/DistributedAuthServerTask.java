package org.infinispan.server.test.task.servertask;

import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

import java.util.Optional;

/**
 * Server task working in Dist mode for specified role.
 *
 * @author Anna Manukyan
 */
public class DistributedAuthServerTask implements ServerTask {
    public static final String NAME = "serverTask_distributed_authentication";
    public static final String CACHE_NAME = "customTaskCache";
    public static final String ALLOWED_ROLE = "executor";

    private TaskContext taskContext;

    @Override
    public void setTaskContext(TaskContext taskContext) {
        this.taskContext = taskContext;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public TaskExecutionMode getExecutionMode() {
        return TaskExecutionMode.ALL_NODES;
    }

    @Override
    public Optional<String> getAllowedRole() {
        return Optional.of(ALLOWED_ROLE);
    }

    @Override
    public Object call() throws Exception {
        return System.getProperty("jboss.node.name");
    }
}
