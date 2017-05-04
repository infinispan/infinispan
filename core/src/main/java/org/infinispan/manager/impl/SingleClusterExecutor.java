package org.infinispan.manager.impl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddressCache;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.ResponseMode;

/**
 * Cluster executor implementation that sends requests to a single node at a time
 *
 * @author wburns
 * @since 9.1
 */
class SingleClusterExecutor extends AbstractClusterExecutor<SingleClusterExecutor> {

   private static final Log log = LogFactory.getLog(SingleClusterExecutor.class);
   private static final boolean isTrace = log.isTraceEnabled();

   SingleClusterExecutor(Predicate<? super Address> predicate, EmbeddedCacheManager manager,
         JGroupsTransport transport, long time, TimeUnit unit, Executor localExecutor,
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

   private org.jgroups.Address findTarget() {
      List<org.jgroups.Address> possibleTargets = getJGroupsTargets(true);
      org.jgroups.Address target;
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
      org.jgroups.Address target = findTarget();
      if (target != null) {
         if (isTrace) {
            log.tracef("Submitting runnable to single remote node - JGroups Address %s", target);
         }
         if (target == convertToJGroupsAddress(me)) {
            // Interrupt does nothing
            super.execute(runnable);
         } else {
            transport.getCommandAwareRpcDispatcher().invokeRemoteCommand(target, new ReplicableCommandRunnable(runnable),
                  ResponseMode.GET_NONE, unit.toMillis(time), DeliverOrder.NONE);
         }
      }
   }

   @Override
   public CompletableFuture<Void> submit(Runnable command) {
      org.jgroups.Address target = findTarget();
      if (target == null) {
         return CompletableFutures.completedExceptionFuture(new SuspectException("No available nodes!"));
      }
      if (isTrace) {
         log.tracef("Submitting runnable to single remote node - JGroups Address %s", target);
      }
      CompletableFuture<Void> future = new CompletableFuture<>();
      if (target == convertToJGroupsAddress(me)) {
         return super.submit(command);
      } else {
         transport.getCommandAwareRpcDispatcher().invokeRemoteCommand(target, new ReplicableCommandRunnable(command),
               ResponseMode.GET_FIRST, unit.toMillis(time), DeliverOrder.NONE).whenComplete((r, t) -> {
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
      org.jgroups.Address target = findTarget();
      if (target == null) {
         return CompletableFutures.completedExceptionFuture(new SuspectException("No available nodes!"));
      }
      if (isTrace) {
         log.tracef("Submitting runnable to single remote node - JGroups Address %s", target);
      }
      if (target == convertToJGroupsAddress(me)) {
         return super.submitConsumer(function, triConsumer);
      } else {
         CompletableFuture<Void> future = new CompletableFuture<>();
         transport.getCommandAwareRpcDispatcher().invokeRemoteCommand(target,
               new ReplicableCommandManagerFunction(function), ResponseMode.GET_ALL, unit.toMillis(time),
               DeliverOrder.NONE).whenComplete((r, t) -> {
            try {
               if (t != null) {
                  triConsumer.accept(JGroupsAddressCache.fromJGroupsAddress(target), null, t);
               } else {
                  consumeResponse(r, target, v -> triConsumer.accept(JGroupsAddressCache.fromJGroupsAddress(target), (V) v, null),
                        throwable -> triConsumer.accept(JGroupsAddressCache.fromJGroupsAddress(target), null, throwable), future::completeExceptionally);
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
