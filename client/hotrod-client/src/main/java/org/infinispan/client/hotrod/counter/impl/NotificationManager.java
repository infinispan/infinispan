package org.infinispan.client.hotrod.counter.impl;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.Handle;

/**
 * A Hot Rod client notification manager for a single {@link CounterManager}.
 * <p>
 * This handles all the users listeners.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class NotificationManager {

   private static final Log log = LogFactory.getLog(NotificationManager.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private final ConnectionManager connectionManager;
   private final Map<String, List<HandleImpl<?>>> clientListeners;

   NotificationManager(CounterOperationFactory factory) {
      this.connectionManager = new ConnectionManager(factory, this::onEvent);
      clientListeners = new ConcurrentHashMap<>();
   }

   public <T extends CounterListener> Handle<T> addListener(String counterName, T listener) {
      if (trace) {
         log.tracef("Add listener for counter '%s'", counterName);
      }
      HandleImpl<T> handle = new HandleImpl<>(counterName, listener);
      clientListeners.compute(counterName, (name, list) -> addToList(name, list, handle));
      return handle;
   }

   public void stop() {
      if (clientListeners.isEmpty()) {
         return;
      }
      clientListeners.clear();
      connectionManager.stop();
   }

   void failedServer(Set<SocketAddress> socketAddresses) {
      connectionManager.failedServers(socketAddresses);
   }

   private void onEvent(HotRodCounterEvent event) {
      if (trace) {
         log.tracef("Received counter event %s", event);
      }
      clientListeners.getOrDefault(event.getCounterName(), Collections.emptyList())
            .parallelStream()
            .forEach(handle -> handle.onEvent(event));
   }

   private List<HandleImpl<?>> addToList(String counterName, List<HandleImpl<?>> list, HandleImpl<?> handle) {
      if (list == null) {
         connectionManager.addConnection(counterName);
         list = new CopyOnWriteArrayList<>();
      }
      list.add(handle);
      return list;
   }

   private void removeListener(String counterName, HandleImpl<?> handle) {
      if (trace) {
         log.tracef("Remove listener for counter '%s'", counterName);
      }
      clientListeners.compute(counterName, (name, list) -> removeFromList(name, list, handle));
   }

   private List<HandleImpl<?>> removeFromList(String counterName, List<HandleImpl<?>> list, HandleImpl<?> handle) {
      list.remove(handle);
      if (list.isEmpty()) {
         list = null;
         connectionManager.removeConnection(counterName);
      }
      return list;
   }

   private class HandleImpl<T extends CounterListener> implements Handle<T> {

      private final T listener;
      private final String counterName;

      private HandleImpl(String counterName, T listener) {
         this.counterName = counterName;
         this.listener = listener;
      }

      @Override
      public T getCounterListener() {
         return listener;
      }

      @Override
      public void remove() {
         removeListener(counterName, this);
      }

      private void onEvent(CounterEvent event) {
         try {
            listener.onUpdate(event);
         } catch (Throwable t) {
            log.debug("Exception in user listener", t);
         }
      }
   }
}
