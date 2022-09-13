package org.infinispan.server.tasks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.tasks.TaskContext;
import org.infinispan.util.function.TriConsumer;

/**
 * @author Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 */
public class DistributedServerTaskRunner implements ServerTaskRunner {

   public DistributedServerTaskRunner() {
   }

   @Override
   public <T> CompletableFuture<T> execute(String taskName, TaskContext context) {
      Cache<?, ?> masterCacheNode = context.getCache().get();

      ClusterExecutor clusterExecutor = SecurityActions.getClusterExecutor(context.getCacheManager());

      List<T> results = new ArrayList<>();
      TriConsumer<Address, T, Throwable> triConsumer = (a, v, t) -> {
         if (t != null) {
            throw new CacheException(t);
         }
         synchronized (results) {
            results.add(v);
         }
      };
      List<TaskParameter> taskParams = context.getParameters().orElse(Collections.emptyMap())
            .entrySet().stream().map(e -> new TaskParameter(e.getKey(), e.getValue().toString())).collect(Collectors.toList());
      CompletableFuture<Void> future = clusterExecutor.submitConsumer(
            new DistributedServerTask<>(taskName, masterCacheNode.getName(), taskParams),
            triConsumer
      );

      return (CompletableFuture<T>) future.thenApply(ignore -> results);
   }

}
