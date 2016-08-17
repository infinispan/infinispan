package org.infinispan.server.test.task.servertask;

import java.util.Map;
import java.util.Optional;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

/**
 * Task verifying that the marshaller is passed properly and the value is properly put into the cache.
 *
 * @author Anna Manukyan
 */
public class DistributedCacheUsingTask implements ServerTask {

    public static final String NAME = "serverTask_distributed_cacheUsage";
    public static final String CACHE_NAME = "customTaskReplTx";
    public static final String VALUE_PREFIX = "modified:";
    public static final String PARAM_KEY = "param";

    private TaskContext taskContext;

    @Override
    @SuppressWarnings("unchecked")
    public Object call() throws Exception {
        Cache<Object, Object> cache = (Cache<Object, Object>) taskContext.getCache().get();
        Map<String, String> parameters = (Map<String, String>) taskContext.getParameters().get();

        assert taskContext.getMarshaller().isPresent();
        Map.Entry<Object, Object> entry = cache.getCacheManager().getCache(CACHE_NAME).entrySet().iterator().next();

        TransactionManager transactionManager = cache.getAdvancedCache().getTransactionManager();
        transactionManager.begin();
        cache.getCacheManager().getCache(CACHE_NAME).getAdvancedCache().lock(entry.getKey());

        cache.getCacheManager().getCache(CACHE_NAME).put(entry.getKey(),
                VALUE_PREFIX + entry.getValue() + ":" + parameters.get(PARAM_KEY));
        transactionManager.commit();
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
