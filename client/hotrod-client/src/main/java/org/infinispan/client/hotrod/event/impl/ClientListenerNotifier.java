package org.infinispan.client.hotrod.event.impl;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;

/**
 * @author Galder Zamarreño
 */
// Note: this class was moved to impl package as it was not meant to be public
public class ClientListenerNotifier {
   private static final Log log = LogFactory.getLog(ClientListenerNotifier.class);
   public static final AtomicInteger counter = new AtomicInteger(0);

   // Time for trying to reconnect listeners when all connections are down.
   public static final int RECONNECT_PERIOD = 5000;

   private final ConcurrentMap<WrappedByteArray, EventDispatcher<?>> dispatchers = new ConcurrentHashMap<>();
   // Dispatchers removed from the map because failover removed their last address, which are awaiting a reconnect.
   // Tracked so that a user removal of the listener can cancel the pending reconnect.
   private final ConcurrentMap<WrappedByteArray, EventDispatcher<?>> pendingFailover = new ConcurrentHashMap<>();
   private final ScheduledThreadPoolExecutor reconnectExecutor;

   private final Marshaller marshaller;
   private final ClassAllowList allowList;

   public ClientListenerNotifier(Marshaller marshaller, Configuration configuration) {
      this.marshaller = marshaller;
      this.allowList = configuration.getClassAllowList();

      TypedProperties defaultAsyncExecutorProperties = configuration.asyncExecutorFactory().properties();
      ConfigurationProperties cp = new ConfigurationProperties(defaultAsyncExecutorProperties);
      final String threadNamePrefix = cp.getDefaultExecutorFactoryThreadNamePrefix();
      final String threadNameSuffix = cp.getDefaultExecutorFactoryThreadNameSuffix();
      reconnectExecutor = new ScheduledThreadPoolExecutor(1, r -> {
         // Reuse the DefaultAsyncExecutorFactory thread name settings
         Thread th = new Thread(r, threadNamePrefix + "-" + counter.getAndIncrement() + threadNameSuffix);
         th.setDaemon(true);
         return th;
      });
      reconnectExecutor.setKeepAliveTime(2 * RECONNECT_PERIOD, TimeUnit.MILLISECONDS);
      reconnectExecutor.allowCoreThreadTimeOut(true);
   }

   public Marshaller marshaller() {
      return marshaller;
   }

   public <C> EventDispatcher<?> addDispatcher(byte[] listenerId, SocketAddress address, C context,
                                               BiFunction<byte[], C, ? extends EventDispatcher<?>> factory) {
      WrappedByteArray key = new WrappedByteArray(listenerId);
      EventDispatcher<?> dispatcher = dispatchers.compute(key, (k, existing) -> {
         if (existing != null) {
            if (address != null) {
               existing.addAddress(address);
            }
            return existing;
         }
         EventDispatcher<?> created = factory.apply(listenerId, context);
         if (log.isTraceEnabled())
            log.tracef("Add dispatcher %s for client listener with id %s, for listener %s",
                  created, Util.printArray(listenerId), created.listener);
         if (address != null) {
            created.addAddress(address);
         }
         return created;
      });
      return dispatcher;
   }

   public void addDispatcher(EventDispatcher<?> dispatcher) {
      WrappedByteArray key = new WrappedByteArray(dispatcher.listenerId);
      dispatchers.compute(key, (k, existing) -> {
         if (existing != null) {
            for (SocketAddress address : dispatcher.addresses()) {
               existing.addAddress(address);
            }
            return existing;
         }
         return dispatcher;
      });
   }

   public void replaceAddress(byte[] listenerId, SocketAddress oldAddress, SocketAddress newAddress) {
      dispatchers.computeIfPresent(new WrappedByteArray(listenerId), (k, dispatcher) -> {
         dispatcher.replaceAddress(oldAddress, newAddress);
         return dispatcher;
      });
   }

   public void addAddress(byte[] listenerId, SocketAddress address) {
      dispatchers.computeIfPresent(new WrappedByteArray(listenerId), (k, dispatcher) -> {
         dispatcher.addAddress(address);
         return dispatcher;
      });
   }

   public void removeAddress(byte[] listenerId, SocketAddress address) {
      dispatchers.computeIfPresent(new WrappedByteArray(listenerId), (k, dispatcher) -> {
         dispatcher.removeAddress(address);
         return dispatcher;
      });
   }

   public void failoverClientListener(byte[] listenerId, SocketAddress address) {
      ByRef<Boolean> stoppedByRemoval = new ByRef<>(false);
      EventDispatcher<?> dispatcher;
      if (address != null) {
         dispatcher = removeClientListener(listenerId, address, stoppedByRemoval);
      } else {
         dispatcher = removeClientListener(listenerId);
      }
      if (dispatcher == null) {
         return;
      }
      if (stoppedByRemoval.get()) {
         // Removing the failed address left the dispatcher without addresses so it was stopped; a reconnect must still be attempted
         dispatcher.markStoppedByFailover();
         // The dispatcher is no longer in the dispatchers map, so a concurrent user removal can only reach it through
         // pendingFailover. If it was removed from there in the meantime, the reconnect must not be attempted.
         if (pendingFailover.get(new WrappedByteArray(listenerId)) != dispatcher) {
            dispatcher.clearStoppedByFailover();
         }
      }
      // Invoke failover event callback, if presents
      dispatcher.invokeFailoverEvent(address);
      // Re-execute adding client listener in one of the remaining nodes

      dispatcher.executeFailover(address).whenComplete((ignore, throwable) -> {
         if (throwable != null) {
            if (throwable instanceof RejectedExecutionException) {
               log.debug("Client listener failover rejected, not retrying", throwable);
            } else {
               log.debug("Unable to failover client listener, so ignore connection reset", throwable);
               scheduleReconnect(dispatcher, address);
            }
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("Fallback listener id %s from a failed server %s",
                     Util.printArray(listenerId), address != null ? address : dispatcher.address());
            }
         }
      });
   }

   public void scheduleReconnect(byte[] listenerId, SocketAddress address) {
      EventDispatcher<?> dispatcher = dispatchers.get(new WrappedByteArray(listenerId));
      if (dispatcher != null) {
         scheduleReconnect(dispatcher, address);
      }
   }

   public void scheduleReconnect(EventDispatcher<?> dispatcher, SocketAddress address) {
      ReconnectTask reconnectTask = new ReconnectTask(dispatcher, address);
      ScheduledFuture<?> scheduledFuture = reconnectExecutor.scheduleAtFixedRate(
            reconnectTask, RECONNECT_PERIOD, RECONNECT_PERIOD, TimeUnit.MILLISECONDS);
      reconnectTask.setCancellationFuture(scheduledFuture);
   }

   public void startClientListener(byte[] listenerId) {
      WrappedByteArray key = new WrappedByteArray(listenerId);
      // The listener has been registered again, so the failover is no longer pending
      pendingFailover.remove(key);
      EventDispatcher<?> dispatcher = dispatchers.get(key);
      if (dispatcher != null) {
         dispatcher.start();
      }
   }

   public EventDispatcher<?> removeClientListener(byte[] listenerId) {
      return removeClientListener(new WrappedByteArray(listenerId));
   }

   public EventDispatcher<?> removeClientListener(byte[] listenerId, SocketAddress address) {
      return removeClientListener(listenerId, address, null);
   }

   private EventDispatcher<?> removeClientListener(byte[] listenerId, SocketAddress address, ByRef<Boolean> stoppedByThisCall) {
      WrappedByteArray wrapped = new WrappedByteArray(listenerId);
      ByRef<EventDispatcher<?>> dispatcherRef = new ByRef<>(null);
      EventDispatcher<?> remaining = dispatchers.computeIfPresent(wrapped, (k, dispatcher) -> {
         dispatcherRef.set(dispatcher);
         if (address != null) {
            dispatcher.removeAddress(address);
            if (dispatcher.addresses().isEmpty()) {
               if (stoppedByThisCall != null) {
                  // Track atomically with the removal so a concurrent user removal can cancel the pending reconnect
                  pendingFailover.put(wrapped, dispatcher);
               }
               return null;
            }
            return dispatcher;
         } else {
            return null;
         }
      });
      EventDispatcher<?> dispatcher = dispatcherRef.get();
      if (dispatcher == null) {
         return null;
      }
      if (remaining == null) {
         if (address == null) {
            dispatcher.clearStoppedByFailover();
         }
         // stop() returns true only if this call actually stopped it
         boolean stopped = dispatcher.stop();
         if (stoppedByThisCall != null) {
            stoppedByThisCall.set(stopped);
            if (!stopped) {
               pendingFailover.remove(wrapped, dispatcher);
            }
         }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Remove client listener with id %s on %s", Util.printArray(listenerId), address);
      }
      return dispatcher;
   }

   private EventDispatcher<?> removeClientListener(WrappedByteArray listenerId) {
      EventDispatcher<?> dispatcher = dispatchers.remove(listenerId);
      EventDispatcher<?> pending = pendingFailover.remove(listenerId);
      if (pending != null) {
         // Removed by the user while waiting for a failover reconnect: stop retrying
         pending.clearStoppedByFailover();
      }
      if (dispatcher == null) {
         if (log.isTraceEnabled()) {
            log.tracef("Client listener %s not present (removed concurrently?)", Util.printArray(listenerId.getBytes()));
         }
         return null;
      }
      dispatcher.clearStoppedByFailover();
      dispatcher.stop();
      if (log.isTraceEnabled())
         log.tracef("Remove client listener with id %s", Util.printArray(listenerId.getBytes()));
      return dispatcher;
   }

   public byte[] findListenerId(Object listener) {
      for (EventDispatcher<?> dispatcher : dispatchers.values()) {
         if (dispatcher.listener.equals(listener))
            return dispatcher.listenerId;
      }
      return null;
   }

   public boolean isListenerConnected(byte[] listenerId) {
      EventDispatcher<?> dispatcher = dispatchers.get(new WrappedByteArray(listenerId));
      return dispatcher != null && dispatcher.isRunning() && !dispatcher.addresses().isEmpty();
   }

   public Set<SocketAddress> findAddresses(byte[] listenerId) {
      EventDispatcher<?> dispatcher = dispatchers.get(new WrappedByteArray(listenerId));
      return dispatcher != null ? dispatcher.addresses() : Collections.emptySet();
   }

   public String findCacheName(byte[] listenerId) {
      EventDispatcher<?> dispatcher = dispatchers.get(new WrappedByteArray(listenerId));
      return dispatcher != null ? dispatcher.cacheName : null;
   }

   public Set<Object> getListeners(String cacheName) {
      Set<Object> ret = new HashSet<>(dispatchers.size());
      for (EventDispatcher<?> dispatcher : dispatchers.values()) {
         if (Objects.equals(dispatcher.cacheName, cacheName))
            ret.add(dispatcher.listener);
      }

      return ret;
   }

   public void stop() {
      for (WrappedByteArray listenerId : dispatchers.keySet()) {
         if (log.isTraceEnabled())
            log.tracef("Remote cache manager stopping, remove client listener id %s", Util.printArray(listenerId.getBytes()));

         removeClientListener(listenerId);
      }
      for (WrappedByteArray listenerId : pendingFailover.keySet()) {
         removeClientListener(listenerId);
      }
      reconnectExecutor.shutdownNow();
   }

   @SuppressWarnings("unchecked")
   public <T> void invokeEvent(byte[] listenerId, T event) {
      EventDispatcher<?> dispatcher = dispatchers.get(new WrappedByteArray(listenerId));
      if (dispatcher == null) {
         throw HOTROD.unexpectedListenerId(Util.printArray(listenerId));
      }
      EventDispatcher<T> eventDispatcher = (EventDispatcher<T>) dispatcher;
      eventDispatcher.invokeEvent(event);
   }

   public DataFormat getCacheDataFormat(byte[] listenerId) {
      EventDispatcher<?> dispatcher = dispatchers.get(new WrappedByteArray(listenerId));
      if (dispatcher == null) {
         throw HOTROD.unexpectedListenerId(Util.printArray(listenerId));
      }
      ClientEventDispatcher clientEventDispatcher = (ClientEventDispatcher) dispatcher;
      return clientEventDispatcher.getDataFormat();
   }

   public ClassAllowList allowList() {
      return allowList;
   }

}
