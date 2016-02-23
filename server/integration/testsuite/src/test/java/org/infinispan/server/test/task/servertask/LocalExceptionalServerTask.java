package org.infinispan.server.test.task.servertask;

import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;

/**
 * Server task which throws exception intentionally.
 *
 * @author amanukya
 */
public class LocalExceptionalServerTask implements ServerTask{
    public static final String NAME = "localScript_throwingException";
    public static final String EXCEPTION_MESSAGE = "Intentionally throws an exception.";
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
    public Object call() throws Exception {

        throw new RuntimeException(EXCEPTION_MESSAGE);
    }
}
