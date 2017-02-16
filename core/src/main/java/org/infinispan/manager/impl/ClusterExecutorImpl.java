package org.infinispan.manager.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.commons.CacheException;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsAddressCache;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.remoting.transport.jgroups.SingleResponseFuture;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.function.SerializableFunction;
import org.infinispan.util.function.SerializableRunnable;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.ResponseMode;

/**
 * Cluster executor implementation
 *
 * @author wburns
 * @since 8.2
 */
public class ClusterExecutorImpl implements ClusterExecutor {

   private static final Log log = LogFactory.getLog(ClusterExecutorImpl.class);
   private static final boolean isTrace = log.isTraceEnabled();

   private final Predicate<? super Address> predicate;
   private final EmbeddedCacheManager manager;
   private final JGroupsTransport transport;
   private final long time;
   private final TimeUnit unit;
   private final Executor localExecutor;
   private final Address me;

   public ClusterExecutorImpl(Predicate<? super Address> predicate, EmbeddedCacheManager manager,
           JGroupsTransport transport, long time, TimeUnit unit, Executor localExecutor) {
      this.predicate = predicate;
      this.manager = manager;
      this.transport = transport;
      if (transport != null) {
         this.me = Objects.requireNonNull(transport.getAddress(),
                 "Transport was not started before retrieving a ClusterExecutor!");
      } else {
         // If there was no transport this cache manager cannot have a cluster, so we just always execute
         this.me = null;
      }
      this.time = time;
      this.unit = unit;
      this.localExecutor = localExecutor;
   }

   /**
    * @return the targets we should use for JGroups. This excludes the local node if it is a target.
    */
   private List<org.jgroups.Address> getJGroupsTargets() {
      List<org.jgroups.Address> list;
      if (transport == null) {
         return Collections.emptyList();
      }
      List<Address> ispnMembers = transport.getMembers();
      int size = ispnMembers.size();
      if (size == 0) {
         list = Collections.emptyList();
      } else {
         if (predicate == null) {
            if (size == 1) {
               Address member = ispnMembers.get(0);
               if (member.equals(me)) {
                  list = Collections.emptyList();
               } else {
                  list = Collections.singletonList(convertToJGroupsAddress(member));
               }
            } else {
               list = ispnMembers.stream()
                       .filter(a -> !a.equals(me))
                       .map(ClusterExecutorImpl::convertToJGroupsAddress)
                       .collect(Collectors.toList());
            }
         } else {
            list = ispnMembers.stream()
                    .filter(a -> !a.equals(me))
                    .filter(predicate)
                    .map(ClusterExecutorImpl::convertToJGroupsAddress)
                    .collect(Collectors.toList());
         }
      }
      return list;
   }

   private static org.jgroups.Address convertToJGroupsAddress(Address address) {
      return ((JGroupsAddress) address).getJGroupsAddress();
   }

   private <T> CompletableFuture<T> startLocalInvocation(Function<? super EmbeddedCacheManager, ? extends T> callable) {
      if (me == null || predicate == null || predicate.test(me)) {
         if (isTrace) {
            log.trace("Submitting callable to local node on executor thread! - Usually remote command thread pool");
         }
         return CompletableFuture.supplyAsync(() -> {
            try {
               return callable.apply(manager);
            } catch (Throwable t) {
               handleCallableRuntimeThrowable(t);
               throw new CacheException("Problems invoking command.", t);
            }
         }, localExecutor);
      } else {
         return null;
      }
   }

   private CompletableFuture<Void> startLocalInvocation(Runnable runnable) {
      if (me == null || predicate == null || predicate.test(me)) {
         if (isTrace) {
            log.trace("Submitting runnable to local node on executor thread! - Usually remote command thread pool");
         }
         return CompletableFuture.runAsync(runnable, localExecutor);
      } else {
         return null;
      }
   }

   @Override
   public void execute(Runnable runnable) {
      executeRunnable(runnable, ResponseMode.GET_ALL);
   }

   private void rethrowException(Throwable t) {
      if (t instanceof RuntimeException) {
         throw (RuntimeException) t;
      } else {
         throw new RuntimeException(t);
      }
   }

   private void handleCallableRuntimeThrowable(Throwable t) {
      if (t instanceof RuntimeException) {
         throw (RuntimeException) t;
      } else if (t instanceof Error) {
         throw (Error) t;
      }
   }

   private CompletableFuture<?> executeRunnable(Runnable runnable, ResponseMode mode) {
      CompletableFuture<?> localFuture = startLocalInvocation(runnable);
      List<org.jgroups.Address> targets = getJGroupsTargets();
      int size = targets.size();
      CompletableFuture<?> remoteFuture;
      if (size == 1) {
         org.jgroups.Address target = targets.get(0);
         if (isTrace) {
            log.tracef("Submitting runnable to single remote node - JGroups Address %s", target);
         }
         CommandAwareRpcDispatcher card = transport.getCommandAwareRpcDispatcher();
         remoteFuture = card.invokeRemoteCommand(target, new ReplicableCommandRunnable(runnable), mode,
                 unit.toMillis(time), DeliverOrder.NONE).handle((r, t) -> {
            if (t != null) {
               rethrowException(t);
            }
            if (r.wasReceived()) {
               if (r.hasException()) {
                  rethrowException(r.getException());
               }
            } else if (r.wasSuspected()) {
               throw log.remoteNodeSuspected(JGroupsAddressCache.fromJGroupsAddress(target));
            } else {
               throw log.remoteNodeTimedOut(JGroupsAddressCache.fromJGroupsAddress(target), time, unit);
            }
            return null;
         });
      } else if (size > 1) {
         CommandAwareRpcDispatcher card = transport.getCommandAwareRpcDispatcher();
         remoteFuture = card.invokeRemoteCommands(targets, new ReplicableCommandRunnable(runnable), mode,
                 unit.toMillis(time), null, DeliverOrder.NONE);
      } else if (localFuture != null) {
         return localFuture;
      } else {
         return CompletableFutures.completedExceptionFuture(new SuspectException("No available nodes!"));
      }
      // remoteFuture is guaranteed to be non null at this point
      if (localFuture != null && mode != ResponseMode.GET_NONE) {
         return CompletableFuture.allOf(localFuture, remoteFuture);
      }
      return remoteFuture;
   }

      @Override
   public void execute(SerializableRunnable runnable) {
      execute((Runnable) runnable);
   }

   @Override
   public CompletableFuture<Void> submit(Runnable command) {
      return executeRunnable(command, ResponseMode.GET_ALL).handle((r, t) -> {
         if (t != null) {
            rethrowException(t);
         }
         return null;
      } );
   }

   @Override
   public CompletableFuture<Void> submit(SerializableRunnable runnable) {
      return submit((Runnable) runnable);
   }

   @Override
   public <V> CompletableFuture<Void> submitConsumer(Function<? super EmbeddedCacheManager, ? extends V> function,
                                                     TriConsumer<? super Address, ? super V, ? super Throwable> triConsumer) {
      CompletableFuture<V> localFuture = startLocalInvocation(function);
      if (localFuture != null) {
         localFuture = localFuture.handle((r, t) -> {
            triConsumer.accept(me, r, t);
            return null;
         });
      }
      List<org.jgroups.Address> targets = getJGroupsTargets();
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
            futures[i] = srf.handle((r, t) -> {
               if (t != null) {
                  triConsumer.accept(JGroupsAddressCache.fromJGroupsAddress(target), null, t);
               } else if (r.wasReceived()) {
                  if (r.hasException()) {
                     triConsumer.accept(JGroupsAddressCache.fromJGroupsAddress(target), null, r.getException());
                  } else {
                     Response response = r.getValue();
                     if (response instanceof SuccessfulResponse) {
                        triConsumer.accept(JGroupsAddressCache.fromJGroupsAddress(target),
                                (V) ((SuccessfulResponse) response).getResponseValue(), null);
                     } else if (response instanceof ExceptionResponse) {
                        triConsumer.accept(JGroupsAddressCache.fromJGroupsAddress(target),
                                null, ((ExceptionResponse) response).getException());
                     } else {
                        triConsumer.accept(JGroupsAddressCache.fromJGroupsAddress(target),
                                null, new IllegalStateException("Response was neither successful or an exception!"));
                     }
                  }
               } else if (r.wasSuspected()) {
                  triConsumer.accept(JGroupsAddressCache.fromJGroupsAddress(target), null,
                          new SuspectException());
               } else {
                  // We throw it so it is propagated to the parent CompletableFuture
                  throw new TimeoutException();
               }
               return null;
            });
         }
         CompletableFuture<Void> remoteFutures = CompletableFuture.allOf(futures);
         return localFuture != null ? localFuture.thenCombine(remoteFutures, (t, u) -> null) : remoteFutures;
      } else if (localFuture != null) {
         return localFuture.handle((r, t) -> null);
      } else {
         return CompletableFutures.completedNull();
      }
   }

   @Override
   public <V> CompletableFuture<Void> submitConsumer(SerializableFunction<? super EmbeddedCacheManager, ? extends V> function,
           TriConsumer<? super Address, ? super V, ? super Throwable> triConsumer) {
      return submitConsumer((Function<EmbeddedCacheManager, V>) function, triConsumer);
   }

   @Override
   public ClusterExecutor timeout(long time, TimeUnit unit) {
      if (time <= 0) {
         throw new IllegalArgumentException("Time must be greater than 0!");
      }
      Objects.requireNonNull(unit, "TimeUnit must be non null!");
      if (this.time == time && this.unit == unit) {
         return this;
      }
      return new ClusterExecutorImpl(predicate, manager, transport, time, unit, localExecutor);
   }

   @Override
   public ClusterExecutor filterTargets(Predicate<? super Address> predicate) {
      return new ClusterExecutorImpl(predicate, manager, transport, time, unit, localExecutor);
   }

   @Override
   public ClusterExecutor filterTargets(Collection<Address> addresses) {
      return filterTargets(address -> addresses.contains(address));
   }

   @Override
   public ClusterExecutor noFilter() {
      if (predicate == null) {
         return this;
      }
      return new ClusterExecutorImpl(predicate, manager, transport, time, unit, localExecutor);
   }
}
