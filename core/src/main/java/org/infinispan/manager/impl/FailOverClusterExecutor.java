package org.infinispan.manager.impl;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.manager.ClusterExecutionPolicy;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Cluster Executor that submits to a single node at a time, but allows for automatic failover up to a certain number
 * of times. The subsequent node where the retry is chosen at random.
 * <p>
 * This executor currently only functions properly when using a single submission cluster executor such as
 * {@link LocalClusterExecutor} and {@link SingleClusterExecutor}
 * @author wburns
 * @since 9.1
 */
class FailOverClusterExecutor implements ClusterExecutor {
   private static final Log log = LogFactory.getLog(FailOverClusterExecutor.class);
   private static final boolean isTrace = log.isTraceEnabled();
   private final ClusterExecutor executor;
   private final int failOverCount;

   FailOverClusterExecutor(ClusterExecutor executor, int failOverCount) {
      this.executor = executor;
      this.failOverCount = failOverCount;
   }

   @Override
   public CompletableFuture<Void> submit(Runnable command) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      submit(command, future, failOverCount);
      return future;
   }

   private void submit(Runnable command, CompletableFuture<Void> future, int retriesLeft) {
      if (isTrace) {
         log.tracef("Submitting runnable %s retries left %d", command, retriesLeft);
      }
      executor.submit(command).whenComplete((v, t) -> {
         if (t != null) {
            if (t instanceof TimeoutException) {
               log.tracef("Command %s was met with timeout", command);
               future.completeExceptionally(t);
            } else if (retriesLeft > 0) {
               log.tracef("Retrying command %s - retries left %d", command, retriesLeft);
               submit(command, future, retriesLeft - 1);
            } else {
               log.tracef("No retries left for command %s, passing last exception to user", command);
               future.completeExceptionally(t);
            }
         } else {
            log.tracef("Command %s completed successfully", command);
            future.complete(null);
         }
      });
   }

   @Override
   public <V> CompletableFuture<Void> submitConsumer(Function<? super EmbeddedCacheManager, ? extends V> callable,
         TriConsumer<? super Address, ? super V, ? super Throwable> triConsumer) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      submitConsumer(callable, triConsumer, future, failOverCount);
      return future;
   }

   private <V> void submitConsumer(Function<? super EmbeddedCacheManager, ? extends V> function,
         TriConsumer<? super Address, ? super V, ? super Throwable> triConsumer, CompletableFuture<Void> future,
         int retriesLeft) {
      if (isTrace) {
         log.tracef("Submitting function %d  retries left %d",
               function, retriesLeft);
      }
      executor.submitConsumer(function, triConsumer).whenComplete((v, t) -> {
         if (t != null) {
            if (t instanceof TimeoutException) {
               log.tracef("Function %s was met with timeout", function);
               future.completeExceptionally(t);
            } else if (retriesLeft > 0) {
               log.tracef("Retrying function %s - retries left %d", function, retriesLeft);
               submitConsumer(function, triConsumer, future, retriesLeft - 1);
            } else {
               log.tracef("No retries left for function %s, passing last exception to user", function);
               future.completeExceptionally(t);
            }
         } else {
            log.tracef("Function %s completed successfully", function);
            future.complete(null);
         }
      });
   }

   @Override
   public ClusterExecutor timeout(long time, TimeUnit unit) {
      ClusterExecutor newExecutor = executor.timeout(time, unit);
      if (newExecutor == executor) {
         return this;
      }
      return new FailOverClusterExecutor(newExecutor, failOverCount);
   }

   @Override
   public ClusterExecutor singleNodeSubmission() {
      return executor;
   }

   @Override
   public ClusterExecutor singleNodeSubmission(int failOverCount) {
      if (failOverCount == this.failOverCount) {
         return this;
      }
      return new FailOverClusterExecutor(executor, failOverCount);
   }

   @Override
   public ClusterExecutor allNodeSubmission() {
      return executor.allNodeSubmission();
   }

   @Override
   public ClusterExecutor filterTargets(Predicate<? super Address> predicate) {
      ClusterExecutor newExecutor = executor.filterTargets(predicate);
      if (newExecutor == executor) {
         return this;
      }
      return new FailOverClusterExecutor(newExecutor, failOverCount);
   }

   @Override
   public ClusterExecutor filterTargets(ClusterExecutionPolicy policy) throws IllegalStateException {
      ClusterExecutor newExecutor = executor.filterTargets(policy);
      if (newExecutor == executor) {
         return this;
      }
      return new FailOverClusterExecutor(newExecutor, failOverCount);
   }

   @Override
   public ClusterExecutor filterTargets(ClusterExecutionPolicy policy, Predicate<? super Address> predicate) throws IllegalStateException {
      ClusterExecutor newExecutor = executor.filterTargets(policy, predicate);
      if (newExecutor == executor) {
         return this;
      }
      return new FailOverClusterExecutor(newExecutor, failOverCount);
   }

   @Override
   public ClusterExecutor filterTargets(Collection<Address> addresses) {
      ClusterExecutor newExecutor = executor.filterTargets(addresses);
      if (newExecutor == executor) {
         return this;
      }
      return new FailOverClusterExecutor(newExecutor, failOverCount);
   }

   @Override
   public ClusterExecutor noFilter() {
      ClusterExecutor newExecutor = executor.noFilter();
      if (newExecutor == executor) {
         return this;
      }
      return new FailOverClusterExecutor(newExecutor, failOverCount);
   }
}
