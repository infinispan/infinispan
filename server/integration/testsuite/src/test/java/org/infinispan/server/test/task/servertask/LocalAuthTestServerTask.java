package org.infinispan.server.test.task.servertask;

import java.util.Optional;

import org.infinispan.Cache;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;

/**
 * Server task which requires specific role.
 *
 * @author amanukya
 */
public class LocalAuthTestServerTask implements ServerTask {
    public static final String NAME = "localAuthTest";
    public static final String CACHE_NAME = "customTaskCache";
    public static final String KEY = "actionPerformed";
    public static final String ALLOWED_ROLE = "executor";
    public static final String EXECUTED_VALUE = "executed";

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
    public Optional<String> getAllowedRole() {
        return Optional.of(ALLOWED_ROLE);
    }

    @Override
    public Object call() throws Exception {
        Cache cache = taskContext.getCache().get();

        cache.put(KEY, true);

        return EXECUTED_VALUE;
    }

}
