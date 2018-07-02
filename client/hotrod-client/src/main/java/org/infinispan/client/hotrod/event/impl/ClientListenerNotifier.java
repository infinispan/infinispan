package org.infinispan.client.hotrod.event.impl;

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

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.Util;

/**
 * @author Galder Zamarreño
 */
// Note: this class was moved to impl package as it was not meant to be public
public class ClientListenerNotifier {
   private static final Log log = LogFactory.getLog(ClientListenerNotifier.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   // Time for trying to reconnect listeners when all connections are down.
   private static final int RECONNECT_PERIOD = 5000;

   private final ConcurrentMap<WrappedByteArray, EventDispatcher<?>> dispatchers = new ConcurrentHashMap<>();
   private final ScheduledThreadPoolExecutor reconnectExecutor = new ScheduledThreadPoolExecutor(1);

   private final Codec codec;
   private final Marshaller marshaller;
   private final ChannelFactory channelFactory;
   private final ClassWhiteList whitelist;

   public ClientListenerNotifier(Codec codec, Marshaller marshaller, ChannelFactory channelFactory, ClassWhiteList whitelist) {
      this.codec = codec;
      this.marshaller = marshaller;
      this.channelFactory = channelFactory;
      this.whitelist = whitelist;

      reconnectExecutor.setKeepAliveTime(2 * RECONNECT_PERIOD, TimeUnit.MILLISECONDS);
      reconnectExecutor.allowCoreThreadTimeOut(true);
   }

   public Marshaller marshaller() {
      return marshaller;
   }

   public void addDispatcher(EventDispatcher<?> dispatcher) {
      dispatchers.put(new WrappedByteArray(dispatcher.listenerId), dispatcher);
      if (trace)
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
      if (trace && failoverListenerIds.isEmpty())
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

      dispatcher.executeFailover().whenComplete((status, throwable) -> {
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
            if (trace) {
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
         if (trace) {
            log.tracef("Client listener %s not present (removed concurrently?)", Util.printArray(listenerId.getBytes()));
         }
      } else {
         dispatcher.stop();
      }
      if (trace)
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
         if (trace)
            log.tracef("Remote cache manager stopping, remove client listener id %s", Util.printArray(listenerId.getBytes()));

         removeClientListener(listenerId);
      }
   }

   public <T> void invokeEvent(byte[] listenerId, T event) {
      EventDispatcher<T> eventDispatcher = (EventDispatcher<T>) dispatchers.get(new WrappedByteArray(listenerId));
      if (eventDispatcher == null) {
         throw log.unexpectedListenerId(Util.printArray(listenerId));
      }
      eventDispatcher.invokeEvent(event);
   }

   public DataFormat getCacheDataFormat(byte[] listenerId) {
      ClientEventDispatcher clientEventDispatcher = (ClientEventDispatcher) dispatchers.get(new WrappedByteArray(listenerId));
      if (clientEventDispatcher == null) {
         throw log.unexpectedListenerId(Util.printArray(listenerId));
      }
      return clientEventDispatcher.getDataFormat();
   }

   public Codec codec() {
      return codec;
   }

   public ClassWhiteList whitelist() {
      return whitelist;
   }

   public ChannelFactory channelFactory() {
      return channelFactory;
   }
}
