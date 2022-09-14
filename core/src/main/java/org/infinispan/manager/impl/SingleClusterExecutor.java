package org.infinispan.manager.impl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.PassthroughSingleResponseCollector;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.security.Security;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Cluster executor implementation that sends requests to a single node at a time
 *
 * @author wburns
 * @since 9.1
 */
class SingleClusterExecutor extends AbstractClusterExecutor<SingleClusterExecutor> {

   private static final Log log = LogFactory.getLog(SingleClusterExecutor.class);

   SingleClusterExecutor(Predicate<? super Address> predicate, EmbeddedCacheManager manager,
         Transport transport, long time, TimeUnit unit, Executor localExecutor,
         ScheduledExecutorService timeoutExecutor) {
      super(predicate, manager, transport, time, unit, localExecutor, timeoutExecutor);
   }

   @Override
   public Log getLog() {
      return log;
   }

   @Override
   protected SingleClusterExecutor sameClusterExecutor(Predicate<? super Address> predicate, long time, TimeUnit unit) {
      return new SingleClusterExecutor(predicate, manager, transport, time, unit, localExecutor, timeoutExecutor);
   }

   private Address findTarget() {
      List<Address> possibleTargets = getRealTargets(true);
      Address target;
      int size = possibleTargets.size();
      if (size == 0) {
         target = null;
      } else if (size == 1) {
         target = possibleTargets.get(0);
      } else {
         target = possibleTargets.get(ThreadLocalRandom.current().nextInt(size));
      }
      return target;
   }

   @Override
   public void execute(Runnable runnable) {
      Address target = findTarget();
      if (target != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Submitting runnable to single remote node - JGroups Address %s", target);
         }
         if (target == me) {
            // Interrupt does nothing
            super.execute(runnable);
         } else {
            try {
               ReplicableCommand command = new ReplicableRunnableCommand(runnable);
               transport.sendTo(target, command, DeliverOrder.NONE);
            } catch (Exception e) {
               throw new CacheException(e);
            }
         }
      }
   }

   @Override
   public CompletableFuture<Void> submit(Runnable runnable) {
      Address target = findTarget();
      if (target == null) {
         return CompletableFutures.completedExceptionFuture(new SuspectException("No available nodes!"));
      }
      if (log.isTraceEnabled()) {
         log.tracef("Submitting runnable to single remote node - JGroups Address %s", target);
      }
      CompletableFuture<Void> future = new CompletableFuture<>();
      if (target == me) {
         return super.submit(runnable);
      } else {
         ReplicableCommand command = new ReplicableRunnableCommand(runnable);
         CompletionStage<Response> request =
            transport.invokeCommand(target, command, PassthroughSingleResponseCollector.INSTANCE, DeliverOrder.NONE,
                                    time, unit);
         request.whenComplete((r, t) -> {
            if (t != null) {
               future.completeExceptionally(t);
            } else {
               consumeResponse(r, target, future::completeExceptionally);
               future.complete(null);
            }
         });
      }
      return future;
   }

   @Override
   public <V> CompletableFuture<Void> submitConsumer(Function<? super EmbeddedCacheManager, ? extends V> function,
         TriConsumer<? super Address, ? super V, ? super Throwable> triConsumer) {
      Address target = findTarget();
      if (target == null) {
         return CompletableFutures.completedExceptionFuture(new SuspectException("No available nodes!"));
      }
      if (log.isTraceEnabled()) {
         log.tracef("Submitting runnable to single remote node - JGroups Address %s", target);
      }
      if (target == me) {
         return super.submitConsumer(function, triConsumer);
      } else {
         CompletableFuture<Void> future = new CompletableFuture<>();
         ReplicableCommand command = new ReplicableManagerFunctionCommand(function, Security.getSubject());
         CompletionStage<Response> request =
            transport.invokeCommand(target, command, PassthroughSingleResponseCollector.INSTANCE, DeliverOrder.NONE,
                                    time, unit);
         request.whenComplete((r, t) -> {
            try {
               if (t != null) {
                  if (t instanceof TimeoutException) {
                     // Consumers for individual nodes should not be able to obscure the timeout
                     future.completeExceptionally(getLog().remoteNodeTimedOut(target, time, unit));
                  } else {
                     triConsumer.accept(target, null, t);
                  }
               } else {
                  consumeResponse(r, target, v -> triConsumer.accept(target, (V) v, null),
                        throwable -> triConsumer.accept(target, null, throwable));
               }
               future.complete(null);
            } catch (Throwable throwable) {
               future.completeExceptionally(throwable);
            }
         });
         return future;
      }
   }

   @Override
   public ClusterExecutor singleNodeSubmission() {
      return this;
   }

   @Override
   public ClusterExecutor singleNodeSubmission(int failOverCount) {
      if (failOverCount == 0) {
         return this;
      }
      return ClusterExecutors.singleNodeSubmissionExecutor(predicate, manager, transport, time, unit, localExecutor,
            timeoutExecutor, failOverCount);
   }

   @Override
   public ClusterExecutor allNodeSubmission() {
      return ClusterExecutors.allSubmissionExecutor(predicate, manager, transport, time, unit, localExecutor,
            timeoutExecutor);
   }
}
