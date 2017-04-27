package org.infinispan.manager.impl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsAddressCache;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.remoting.transport.jgroups.SingleResponseFuture;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.ResponseMode;

/**
 * Cluster executor implementation that sends a request to all available nodes
 *
 * @author wburns
 * @since 8.2
 */
class AllClusterExecutor extends AbstractClusterExecutor<AllClusterExecutor> {

   private static final Log log = LogFactory.getLog(AllClusterExecutor.class);
   private static final boolean isTrace = log.isTraceEnabled();

   AllClusterExecutor(Predicate<? super Address> predicate, EmbeddedCacheManager manager,
         JGroupsTransport transport, long time, TimeUnit unit, Executor localExecutor,
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
         if (isTrace) {
            log.trace("Submitting callable to local node on executor thread! - Usually remote command thread pool");
         }
         return super.submitConsumer(callable, triConsumer);
      } else {
         return null;
      }
   }

   protected CompletableFuture<Void> startLocalInvocation(Runnable runnable) {
      if (me == null || predicate == null || predicate.test(me)) {
         if (isTrace) {
            log.trace("Submitting runnable to local node on executor thread! - Usually remote command thread pool");
         }
         return super.submit(runnable);
      } else {
         return null;
      }
   }

   @Override
   public void execute(Runnable runnable) {
      executeRunnable(runnable, ResponseMode.GET_ALL);
   }

   private CompletableFuture<?> executeRunnable(Runnable runnable, ResponseMode mode) {
      CompletableFuture<?> localFuture = startLocalInvocation(runnable);
      List<org.jgroups.Address> targets = getJGroupsTargets(false);
      int size = targets.size();
      CompletableFuture<?> remoteFuture;
      if (size == 1) {
         org.jgroups.Address target = targets.get(0);
         if (isTrace) {
            log.tracef("Submitting runnable to single remote node - JGroups Address %s", target);
         }
         CommandAwareRpcDispatcher card = transport.getCommandAwareRpcDispatcher();
         remoteFuture = new CompletableFuture<>();
         card.invokeRemoteCommand(target, new ReplicableCommandRunnable(runnable), mode,
                 unit.toMillis(time), DeliverOrder.NONE).handle((r, t) -> {
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
         CommandAwareRpcDispatcher card = transport.getCommandAwareRpcDispatcher();
         remoteFuture = new CompletableFuture<>();
         card.invokeRemoteCommands(targets, new ReplicableCommandRunnable(runnable), mode,
                 unit.toMillis(time), null, DeliverOrder.NONE).handle((r, t) -> {
            if (t != null) {
               remoteFuture.completeExceptionally(t);
            } else {
               r.forEach(e -> consumeResponse(e.getValue(), e.getKey(), remoteFuture::completeExceptionally));
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
      if (localFuture != null && mode != ResponseMode.GET_NONE) {
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
      executeRunnable(command, ResponseMode.GET_ALL).handle((r, t) -> {
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
      List<org.jgroups.Address> targets = getJGroupsTargets(false);
      int size = targets.size();
      if (size > 0) {
         CompletableFuture<?>[] futures = new CompletableFuture[size];
         for (int i = 0; i < size; ++i) {
            CommandAwareRpcDispatcher card = transport.getCommandAwareRpcDispatcher();
            org.jgroups.Address target = targets.get(i);
            if (isTrace) {
               log.tracef("Submitting consumer to single remote node - JGroups Address %s", target);
            }
            SingleResponseFuture srf = card.invokeRemoteCommand(target, new ReplicableCommandManagerFunction(function),
                    ResponseMode.GET_ALL, unit.toMillis(time), DeliverOrder.NONE);
            futures[i] = srf.whenComplete((r, t) -> {
               if (t != null) {
                  triConsumer.accept(JGroupsAddressCache.fromJGroupsAddress(target), null, t);
               } else {
                  consumeResponse(r, target, v -> triConsumer.accept(JGroupsAddressCache.fromJGroupsAddress(target), (V) v, null),
                        // TODO: check if obtrude works here?
                        throwable -> triConsumer.accept(JGroupsAddressCache.fromJGroupsAddress(target), null, throwable), srf::obtrudeException);
               }
            });
         }
         CompletableFuture<Void> resultFuture = new CompletableFuture<>();
         CompletableFuture<Void> remoteFutures = CompletableFuture.allOf(futures);
         (localFuture != null ? localFuture.thenCombine(remoteFutures, (t, u) -> null) : remoteFutures).whenComplete((v, t) -> {
            if (t != null) {
               if (t instanceof CompletionException) {
                  resultFuture.completeExceptionally(t.getCause());
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
