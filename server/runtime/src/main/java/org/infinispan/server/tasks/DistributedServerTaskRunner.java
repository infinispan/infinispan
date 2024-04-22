package org.infinispan.server.tasks;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.Security;
import org.infinispan.tasks.TaskContext;
import org.infinispan.util.function.TriConsumer;

/**
 * @author Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 */
public final class DistributedServerTaskRunner implements ServerTaskRunner {

   private static final DistributedServerTaskRunner INSTANCE = new DistributedServerTaskRunner();

   private DistributedServerTaskRunner() { }

   public static DistributedServerTaskRunner getInstance() {
      return INSTANCE;
   }

   @Override
   public <T> CompletableFuture<T> execute(ServerTaskWrapper<T> task, TaskContext context) {
      String cacheName = context.getCache().map(Cache::getName).orElse(null);

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
      CompletableFuture<Void> future = Security.doAs(context.subject(), (PrivilegedAction<CompletableFuture<Void>>) () -> clusterExecutor.submitConsumer(
            new DistributedServerTask<>(task.getName(), cacheName, context),
            triConsumer
      ));
      return (CompletableFuture<T>) future.thenApply(ignore -> results);
   }
}
