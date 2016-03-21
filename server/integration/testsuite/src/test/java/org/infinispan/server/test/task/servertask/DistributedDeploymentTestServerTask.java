package org.infinispan.server.test.task.servertask;

import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

/**
 * Server Task used while testing the servertask deploy/undeploy on the server.
 *
 * @author Anna Manukyan
 */
public class DistributedDeploymentTestServerTask implements ServerTask {

    public static final String NAME = "server_task_deployment_test";
    private TaskContext taskContext;

    @Override
    public Object call() {
        return System.getProperty("jboss.node.name");
    }

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
}