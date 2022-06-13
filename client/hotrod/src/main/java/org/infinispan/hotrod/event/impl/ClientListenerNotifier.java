package org.infinispan.hotrod.event.impl;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.hotrod.impl.ConfigurationProperties;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ChannelFactory;

/**
 */
// Note: this class was moved to impl package as it was not meant to be public
public class ClientListenerNotifier {
   private static final Log log = LogFactory.getLog(ClientListenerNotifier.class, Log.class);
   public static final AtomicInteger counter = new AtomicInteger(0);

   // Time for trying to reconnect listeners when all connections are down.
   public static final int RECONNECT_PERIOD = 5000;

   private final ConcurrentMap<WrappedByteArray, EventDispatcher<?>> dispatchers = new ConcurrentHashMap<>();
   private final ScheduledThreadPoolExecutor reconnectExecutor;

   private final Codec codec;
   private final ChannelFactory channelFactory;
   private final ClassAllowList allowList;

   public ClientListenerNotifier(Codec codec, ChannelFactory channelFactory, HotRodConfiguration configuration) {
      this.codec = codec;
      this.channelFactory = channelFactory;
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

   public void addDispatcher(EventDispatcher<?> dispatcher) {
      dispatchers.put(new WrappedByteArray(dispatcher.listenerId), dispatcher);
      if (log.isTraceEnabled())
         log.tracef("Add dispatcher %s for client listener with id %s, for listener %s",
               dispatcher, Util.printArray(dispatcher.listenerId), dispatcher.listener);
   }

   public void failoverListeners(Set<SocketAddress> failedServers) {
      // Compile all listener ids that need failing over
      List<WrappedByteArray> failoverListenerIds = new ArrayList<>();
      for (Map.Entry<WrappedByteArray, EventDispatcher<?>> entry : dispatchers.entrySet()) {
         EventDispatcher<?> dispatcher = entry.getValue();
         if (failedServers.contains(dispatcher.address()))
            failoverListenerIds.add(entry.getKey());
      }
      if (log.isTraceEnabled() && failoverListenerIds.isEmpty())
         log.tracef("No event listeners registered in failed servers: %s", failedServers);

      // Remove tracking listeners and read to the fallback transport
      failoverListenerIds.forEach(wrapped -> failoverClientListener(wrapped.getBytes()));
   }

   public void failoverClientListener(byte[] listenerId) {
      EventDispatcher<?> dispatcher = removeClientListener(listenerId);
      if (dispatcher == null) {
         return;
      }
      // Invoke failover event callback, if presents
      dispatcher.invokeFailoverEvent();
      // Re-execute adding client listener in one of the remaining nodes

      dispatcher.executeFailover().whenComplete((ignore, throwable) -> {
         if (throwable != null) {
            if (throwable instanceof RejectedExecutionException) {
               log.debug("Client listener failover rejected, not retrying", throwable);
            } else {
               log.debug("Unable to failover client listener, so ignore connection reset", throwable);
               ReconnectTask reconnectTask = new ReconnectTask(dispatcher);
               ScheduledFuture<?> scheduledFuture = reconnectExecutor.scheduleAtFixedRate(reconnectTask, RECONNECT_PERIOD, RECONNECT_PERIOD, TimeUnit.MILLISECONDS);
               reconnectTask.setCancellationFuture(scheduledFuture);
            }
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("Fallback listener id %s from a failed server %s",
                     Util.printArray(listenerId), dispatcher.address());
            }
         }
      });
   }

   public void startClientListener(byte[] listenerId) {
      EventDispatcher eventDispatcher = dispatchers.get(new WrappedByteArray(listenerId));
      eventDispatcher.start();
   }

   public EventDispatcher<?> removeClientListener(byte[] listenerId) {
      return removeClientListener(new WrappedByteArray(listenerId));
   }

   private EventDispatcher<?> removeClientListener(WrappedByteArray listenerId) {
      EventDispatcher dispatcher = dispatchers.remove(listenerId);
      if (dispatcher == null) {
         if (log.isTraceEnabled()) {
            log.tracef("Client listener %s not present (removed concurrently?)", Util.printArray(listenerId.getBytes()));
         }
      } else {
         dispatcher.stop();
      }
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
      // If listener not present, is not active
      return dispatcher != null && dispatcher.isRunning();
   }

   public SocketAddress findAddress(byte[] listenerId) {
      EventDispatcher<?> dispatcher = dispatchers.get(new WrappedByteArray(listenerId));
      if (dispatcher != null)
         return dispatcher.address();

      return null;
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

   public <T> void invokeEvent(byte[] listenerId, T event) {
      EventDispatcher<T> eventDispatcher = (EventDispatcher<T>) dispatchers.get(new WrappedByteArray(listenerId));
      if (eventDispatcher == null) {
         throw HOTROD.unexpectedListenerId(Util.printArray(listenerId));
      }
      eventDispatcher.invokeEvent(event);
   }

   public DataFormat getCacheDataFormat(byte[] listenerId) {
      ClientEventDispatcher clientEventDispatcher = (ClientEventDispatcher) dispatchers.get(new WrappedByteArray(listenerId));
      if (clientEventDispatcher == null) {
         throw HOTROD.unexpectedListenerId(Util.printArray(listenerId));
      }
      return clientEventDispatcher.getDataFormat();
   }

   public Codec codec() {
      return codec;
   }

   public ClassAllowList allowList() {
      return allowList;
   }

   public ChannelFactory channelFactory() {
      return channelFactory;
   }
}
