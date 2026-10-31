package org.infinispan.client.hotrod.event.impl;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashSet;
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
      EventDispatcher<?> dispatcher = dispatchers.computeIfAbsent(key, k -> {
         EventDispatcher<?> created = factory.apply(listenerId, context);
         if (log.isTraceEnabled())
            log.tracef("Add dispatcher %s for client listener with id %s, for listener %s",
                  created, Util.printArray(listenerId), created.listener);
         return created;
      });
      if (address != null) {
         dispatcher.addAddress(address);
      }
      return dispatcher;
   }

   public void addDispatcher(EventDispatcher<?> dispatcher) {
      EventDispatcher<?> existing = addDispatcher(dispatcher.listenerId, null, dispatcher, (id, d) -> d);
      for (SocketAddress address : dispatcher.addresses()) {
         existing.addAddress(address);
      }
   }

   public void replaceAddress(byte[] listenerId, SocketAddress oldAddress, SocketAddress newAddress) {
      EventDispatcher<?> dispatcher = dispatchers.get(new WrappedByteArray(listenerId));
      if (dispatcher != null) {
         dispatcher.replaceAddress(oldAddress, newAddress);
      }
   }

   public void addAddress(byte[] listenerId, SocketAddress address) {
      EventDispatcher<?> dispatcher = dispatchers.get(new WrappedByteArray(listenerId));
      if (dispatcher != null) {
         dispatcher.addAddress(address);
      }
   }

   public void removeAddress(byte[] listenerId, SocketAddress address) {
      EventDispatcher<?> dispatcher = dispatchers.get(new WrappedByteArray(listenerId));
      if (dispatcher != null) {
         dispatcher.removeAddress(address);
      }
   }

   public void failoverClientListener(byte[] listenerId, SocketAddress address) {
      EventDispatcher<?> dispatcher;
      if (address != null) {
         dispatcher = removeClientListener(listenerId, address);
      } else {
         dispatcher = removeClientListener(listenerId);
      }
      if (dispatcher == null) {
         return;
      }
      if (!dispatcher.isRunning()) {
         // Removing the failed address left the dispatcher without addresses so it was stopped; a reconnect must still be attempted
         dispatcher.markStoppedByFailover();
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
      EventDispatcher<?> dispatcher = dispatchers.get(new WrappedByteArray(listenerId));
      if (dispatcher != null) {
         dispatcher.start();
      }
   }

   public EventDispatcher<?> removeClientListener(byte[] listenerId) {
      return removeClientListener(new WrappedByteArray(listenerId));
   }

   public EventDispatcher<?> removeClientListener(byte[] listenerId, SocketAddress address) {
      WrappedByteArray wrapped = new WrappedByteArray(listenerId);
      EventDispatcher<?> dispatcher = dispatchers.get(wrapped);
      if (dispatcher == null) {
         return null;
      }
      if (address != null) {
         dispatcher.removeAddress(address);
         if (dispatcher.addresses().isEmpty()) {
            dispatchers.remove(wrapped, dispatcher);
            dispatcher.stop();
         }
      } else {
         dispatchers.remove(wrapped, dispatcher);
         dispatcher.stop();
      }
      if (log.isTraceEnabled()) {
         log.tracef("Remove client listener with id %s on %s", Util.printArray(listenerId), address);
      }
      return dispatcher;
   }

   private EventDispatcher<?> removeClientListener(WrappedByteArray listenerId) {
      EventDispatcher<?> dispatcher = dispatchers.remove(listenerId);
      if (dispatcher == null) {
         if (log.isTraceEnabled()) {
            log.tracef("Client listener %s not present (removed concurrently?)", Util.printArray(listenerId.getBytes()));
         }
         return null;
      }
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
         if (dispatcher.cacheName.equals(cacheName))
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
