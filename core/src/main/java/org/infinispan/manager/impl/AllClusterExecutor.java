package org.infinispan.manager.impl;

import static org.infinispan.commons.util.concurrent.CompletableFutures.asCompletionException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.PassthroughMapResponseCollector;
import org.infinispan.remoting.transport.impl.PassthroughSingleResponseCollector;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.security.Security;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Cluster executor implementation that sends a request to all available nodes
 *
 * @author wburns
 * @since 8.2
 */
class AllClusterExecutor extends AbstractClusterExecutor<AllClusterExecutor> {

   private static final Log log = LogFactory.getLog(AllClusterExecutor.class);

   AllClusterExecutor(Predicate<? super Address> predicate, EmbeddedCacheManager manager,
         Transport transport, long time, TimeUnit unit, Executor localExecutor,
         ScheduledExecutorService timeoutExecutor) {
      super(predicate, manager, transport, time, unit, localExecutor, timeoutExecutor);
   }

   @Override
   public Log getLog() {
      return log;
   }

   @Override
   protected AllClusterExecutor sameClusterExecutor(Predicate<? super Address> predicate, long time, TimeUnit unit) {
      return new AllClusterExecutor(predicate, manager, transport, time, unit, localExecutor, timeoutExecutor);
   }

   private <T> CompletableFuture<Void> startLocalInvocation(Function<? super EmbeddedCacheManager, ? extends T> callable,
         TriConsumer<? super Address, ? super T, ? super Throwable> triConsumer) {
      if (me == null || predicate == null || predicate.test(me)) {
         if (log.isTraceEnabled()) {
            log.trace("Submitting callable to local node on executor thread! - Usually remote command thread pool");
         }
         return super.submitConsumer(callable, triConsumer);
      } else {
         return null;
      }
   }

   protected CompletableFuture<Void> startLocalInvocation(Runnable runnable) {
      if (me == null || predicate == null || predicate.test(me)) {
         if (log.isTraceEnabled()) {
            log.trace("Submitting runnable to local node on executor thread! - Usually remote command thread pool");
         }
         return super.submit(runnable);
      } else {
         return null;
      }
   }

   @Override
   public void execute(Runnable runnable) {
      executeRunnable(runnable);
   }

   private CompletableFuture<?> executeRunnable(Runnable runnable) {
      CompletableFuture<?> localFuture = startLocalInvocation(runnable);
      List<Address> targets = getRealTargets(false);
      int size = targets.size();
      CompletableFuture<?> remoteFuture;
      if (size == 1) {
         Address target = targets.get(0);
         if (log.isTraceEnabled()) {
            log.tracef("Submitting runnable to single remote node - JGroups Address %s", target);
         }
         remoteFuture = new CompletableFuture<>();
         ReplicableCommand command = new ReplicableRunnableCommand(runnable);
         CompletionStage<Response> request = transport.invokeCommand(target, command, PassthroughSingleResponseCollector.INSTANCE, DeliverOrder.NONE, time, unit);
         request.handle((r, t) -> {
            if (t != null) {
               remoteFuture.completeExceptionally(t);
            } else {
               consumeResponse(r, target, remoteFuture::completeExceptionally);
               // This won't override exception if there was one
               remoteFuture.complete(null);
            }
            return null;
         });
      } else if (size > 1) {
         remoteFuture = new CompletableFuture<>();
         ReplicableCommand command = new ReplicableRunnableCommand(runnable);
         ResponseCollector<Map<Address, Response>> collector = new PassthroughMapResponseCollector(targets.size());
         CompletionStage<Map<Address, Response>> request = transport.invokeCommand(targets, command, collector, DeliverOrder.NONE, time, unit);
         request.handle((r, t) -> {
            if (t != null) {
               remoteFuture.completeExceptionally(t);
            } else {
               r.forEach((key, value) -> consumeResponse(value, key, remoteFuture::completeExceptionally));
               remoteFuture.complete(null);
            }
            return null;
         });
      } else if (localFuture != null) {
         return localFuture;
      } else {
         return CompletableFutures.completedExceptionFuture(new SuspectException("No available nodes!"));
      }
      // remoteFuture is guaranteed to be non null at this point
      if (localFuture != null) {
         CompletableFuture<Void> future = new CompletableFuture<>();
         CompletableFuture.allOf(localFuture, remoteFuture).whenComplete((v, t) -> {
            if (t != null) {
               if (t instanceof CompletionException) {
                  future.completeExceptionally(t.getCause());
               } else {
                  future.completeExceptionally(t);
               }
            } else {
               future.complete(null);
            }
         });
         return future;
      }
      return remoteFuture;
   }

   @Override
   public CompletableFuture<Void> submit(Runnable command) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      executeRunnable(command).handle((r, t) -> {
         if (t != null) {
            future.completeExceptionally(t);
         }
         future.complete(null);
         return null;
      });
      return future;
   }

   @Override
   public <V> CompletableFuture<Void> submitConsumer(Function<? super EmbeddedCacheManager, ? extends V> function,
                                                     TriConsumer<? super Address, ? super V, ? super Throwable> triConsumer) {
      CompletableFuture<Void> localFuture = startLocalInvocation(function, triConsumer);
      List<Address> targets = getRealTargets(false);
      int size = targets.size();
      if (size > 0) {
         CompletableFuture<?>[] futures;
         if (localFuture != null) {
            futures = new CompletableFuture[size + 1];
            futures[size] = localFuture;
         } else {
            futures = new CompletableFuture[size];
         }
         for (int i = 0; i < size; ++i) {
            Address target = targets.get(i);
            if (log.isTraceEnabled()) {
               log.tracef("Submitting consumer to single remote node - address=%s, subject=%s", target, Security.getSubject());
            }
            ReplicableCommand command = new ReplicableManagerFunctionCommand(function, Security.getSubject());
            CompletionStage<Response> request = transport.invokeCommand(target, command, PassthroughSingleResponseCollector.INSTANCE, DeliverOrder.NONE, time, unit);
            futures[i] = request.toCompletableFuture().whenComplete((r, t) -> {
               if (t != null) {
                  if (t instanceof TimeoutException) {
                     // Consumers for individual nodes should not be able to obscure the timeout
                     throw asCompletionException(t);
                  } else {
                     triConsumer.accept(target, null, t);
                  }
               } else {
                  consumeResponse(r, target, v -> triConsumer.accept(target, (V) v, null),
                        throwable -> triConsumer.accept(target, null, throwable));
               }
            });
         }
         CompletableFuture<Void> resultFuture = new CompletableFuture<>();
         CompletableFuture<Void> allFuture = CompletableFuture.allOf(futures);
         allFuture.whenComplete((v, t) -> {
            if (t != null) {
               if (t instanceof CompletionException) {
                  resultFuture.completeExceptionally(t.getCause());
               } else {
                  resultFuture.completeExceptionally(t);
               }
            } else {
               resultFuture.complete(null);
            }
         });
         return resultFuture;
      } else if (localFuture != null) {
         return localFuture;
      } else {
         return CompletableFutures.completedExceptionFuture(new SuspectException("No available nodes!"));
      }
   }

   @Override
   public ClusterExecutor singleNodeSubmission() {
      return ClusterExecutors.singleNodeSubmissionExecutor(predicate, manager, transport, time, unit, localExecutor,
            timeoutExecutor, 0);
   }

   @Override
   public ClusterExecutor singleNodeSubmission(int failOverCount) {
      return ClusterExecutors.singleNodeSubmissionExecutor(predicate, manager, transport, time, unit, localExecutor,
            timeoutExecutor, failOverCount);
   }

   @Override
   public ClusterExecutor allNodeSubmission() {
      return this;
   }

}
