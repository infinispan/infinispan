package org.infinispan.server.infinispan.task;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.tasks.TaskContext;
import org.infinispan.util.concurrent.CompletableFutures;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/28/16
 * Time: 9:36 AM
 */
public class DistributedServerTaskRunner implements ServerTaskRunner {
   @Override
   public <T> CompletableFuture<T> execute(String taskName, TaskContext context) {
      Cache<?, ?> masterCacheNode = context.getCache().get();

      DefaultExecutorService des = new DefaultExecutorService(masterCacheNode);
      try {
         List<Future<T>> tasks = des.submitEverywhere(new DistributedServerTask<>(taskName, context.getParameters()));
         List<CompletableFuture<T>> all = new ArrayList<>(tasks.size());
         tasks.forEach(task -> all.add(CompletableFutures.toCompletableFuture((NotifyingFuture<T>) task)));

//         noinspection unchecked
         return (CompletableFuture<T>) CompletableFutures.sequence(all);
      } finally {
         des.shutdown();
      }
   }

}
