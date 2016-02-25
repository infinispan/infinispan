package org.infinispan.server.test.task.servertask;

import org.infinispan.Cache;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * // TODO: Document this
 *
 * @author amanukya
 * @since 4.0
 */
public class DistributedCacheUsingTask implements ServerTask {

    public static final String NAME = "serverTask_distributed_cacheUsage";
    public static final String CACHE_NAME = "customTaskRepl";
    public static final String VALUE_PREFIX = "modified:";
    public static final String PARAM_KEY = "param";

    private TaskContext taskContext;

    @Override
    @SuppressWarnings("unchecked")
    public Object call() throws IOException, ClassNotFoundException {
        Cache<Object, Object> cache = (Cache<Object, Object>) taskContext.getCache().get();
        Map<String, String> parameters = (Map<String, String>) taskContext.getParameters().get();

        assert taskContext.getMarshaller().isPresent();
        Map.Entry<Object, Object> entry = cache.getCacheManager().getCache(CACHE_NAME).entrySet().iterator().next();

        cache.getCacheManager().getCache(CACHE_NAME).put(entry.getKey(), VALUE_PREFIX + entry.getValue() + ":" + parameters.get(PARAM_KEY));
        return null;
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
    public Optional<String> getAllowedRole() {
        return Optional.empty();
    }

    @Override
    public TaskExecutionMode getExecutionMode() {
        return TaskExecutionMode.ALL_NODES;
    }


}
