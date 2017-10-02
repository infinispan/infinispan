package org.infinispan.server.test.task.servertask;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.stream.CacheCollectors;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;

/**
 * MapReduce Server task to run in local mode.
 *
 * @author amanukya
 */
public class LocalMapReduceServerTask implements ServerTask {
    public static final String NAME = "local_mapreduce_task";
    private TaskContext taskContext;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Object call() throws Exception {
        Cache<String, String> cache = (Cache<String, String>) taskContext.getCache().get();

        return cache.entrySet().stream()
                .map((Serializable & Function<Map.Entry<String, String>, String[]>) e -> e.getValue().split("\\s+"))
                .flatMap((Serializable & Function<String[], Stream<String>>) Arrays::stream)
                .collect(() -> Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    @Override
    public void setTaskContext(TaskContext taskContext) {
        this.taskContext = taskContext;
    }
}
