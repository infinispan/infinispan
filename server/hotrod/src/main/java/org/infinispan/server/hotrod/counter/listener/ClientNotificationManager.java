package org.infinispan.server.hotrod.counter.listener;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.ByRef;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Handle;
import org.infinispan.server.hotrod.logging.Log;

import io.netty.channel.Channel;

/**
 * A client notification manager.
 * <p>
 * A client is identified by its {@code listener-id} and it uses the same channel to send all the counter related
 * events. The client can use the same {@code listener-id} for all its requests.
 * <p>
 * It only register a single listener for each counter. So, if multiple requests are received for the same counter, only
 * one will succeed.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
class ClientNotificationManager {

   private static final Log log = LogFactory.getLog(ClientNotificationManager.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Map<String, Handle<Listener>> counterListener;
   private final CounterManager counterManager;
   private final AtomicLong messageId;
   private final Channel channel;
   private final Queue<ClientCounterEvent> eventQueue;
   private final byte[] listenerId;

   ClientNotificationManager(byte[] listenerId, CounterManager counterManager, Channel channel) {
      this.listenerId = listenerId;
      this.counterManager = counterManager;
      this.channel = channel;
      counterListener = new ConcurrentHashMap<>();
      messageId = new AtomicLong();
      eventQueue = new ConcurrentLinkedQueue<>();
   }

   boolean addCounterListener(byte version, String counterName) {
      if (trace) {
         log.tracef("Add listener for counter '%s'", counterName);
      }
      ByRef<Boolean> status = new ByRef<>(true);
      counterListener.computeIfAbsent(counterName, name -> createListener(version, name, status));
      return status.get();
   }

   void removeCounterListener(String counterName) {
      if (trace) {
         log.tracef("Remove listener for counter '%s'", counterName);
      }
      counterListener.computeIfPresent(counterName, (name, handle) -> {
         handle.remove();
         return null;
      });
   }

   boolean isEmpty() {
      return counterListener.isEmpty();
   }

   void removeAll() {
      if (trace) {
         log.trace("Remove all listeners");
      }
      counterListener.values().forEach(Handle::remove);
      counterListener.clear();
   }

   void channelActive(Channel otherChannel) {
      boolean sameChannel = this.channel == otherChannel;
      if (trace) {
         log.tracef("Channel active! is same channel? %s", sameChannel);
      }
      if (sameChannel) {
         sendEvents();
      }
   }

   private void sendEvents() {
      if (trace) {
         log.tracef("Send events! is writable? %s", channel.isWritable());
      }
      ClientCounterEvent event;
      boolean written = false;
      while (channel.isWritable() && (event = eventQueue.poll()) != null) {
         if (trace) {
            log.tracef("Sending event %s", event);
         }
         channel.write(event);
         written = true;
      }
      if (written) {
         channel.flush();
      }
   }

   private Handle<Listener> createListener(byte version, String counterName, ByRef<Boolean> status) {
      CounterConfiguration configuration = counterManager.getConfiguration(counterName);
      if (configuration == null) {
         status.set(false);
         return null;
      }
      Handle<Listener> handle;
      if (configuration.type() == CounterType.WEAK) {
         handle = counterManager.getWeakCounter(counterName).addListener(new Listener(counterName, version));
      } else {
         handle = counterManager.getStrongCounter(counterName).addListener(new Listener(counterName, version));
      }
      status.set(true);
      return handle;
   }

   private void trySendEvents() {
      boolean writable = channel.isWritable();
      if (trace) {
         log.tracef("Try to send events after notification. is writable? %s", writable);
      }
      if (channel.isWritable()) {
         channel.eventLoop().execute(this::sendEvents);
      }
   }

   private class Listener implements CounterListener {

      private final String counterName;
      private final byte version;

      private Listener(String counterName, byte version) {
         this.counterName = counterName;
         this.version = version;
      }

      @Override
      public void onUpdate(CounterEvent entry) {
         if (trace) {
            log.tracef("Event received! %s", entry);
         }
         eventQueue.add(new ClientCounterEvent(listenerId, messageId.incrementAndGet(), version, counterName, entry));
         trySendEvents();
      }
   }
}
