package org.infinispan.client.hotrod.event.impl;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryExpired;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientCacheFailover;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryExpiredEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientCacheFailoverEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.commons.util.Util;

public final class ClientEventDispatcher extends EventDispatcher<ClientEvent> {

   public static final ClientCacheFailoverEvent FAILOVER_EVENT_SINGLETON = () -> ClientEvent.Type.CLIENT_CACHE_FAILOVER;

   private static final Map<Class<? extends Annotation>, Class<?>[]> allowedListeners = new HashMap<>(5);

   static {
      allowedListeners.put(ClientCacheEntryCreated.class, new Class[]{ClientCacheEntryCreatedEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheEntryModified.class, new Class[]{ClientCacheEntryModifiedEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheEntryRemoved.class, new Class[]{ClientCacheEntryRemovedEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheEntryExpired.class, new Class[]{ClientCacheEntryExpiredEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheFailover.class, new Class[]{ClientCacheFailoverEvent.class});
   }

   private final Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables;

   private final ClientListenerOperation op;
   private final InternalRemoteCache<?, ?> remoteCache;

   ClientEventDispatcher(ClientListenerOperation op, SocketAddress address, Map<Class<? extends Annotation>,
         List<ClientListenerInvocation>> invocables, String cacheName, Runnable cleanup, InternalRemoteCache<?, ?> remoteCache) {
      super(cacheName, op.listener, op.listenerId, address, cleanup);
      this.op = op;
      this.invocables = invocables;
      this.remoteCache = remoteCache;
   }

   public static ClientEventDispatcher create(ClientListenerOperation op, SocketAddress address, Runnable cleanup,
                                              InternalRemoteCache<?, ?> remoteCache) {
      Map<Class<? extends Annotation>, List<ClientEventDispatcher.ClientListenerInvocation>> invocables = findMethods(op.listener);
      return new ClientEventDispatcher(op, address, invocables, op.getCacheName(), cleanup, remoteCache);
   }

   public static Map<Class<? extends Annotation>, List<ClientEventDispatcher.ClientListenerInvocation>> findMethods(Object listener) {
      Map<Class<? extends Annotation>, List<ClientEventDispatcher.ClientListenerInvocation>> listenerMethodMap = new HashMap<>(4, 0.99f);

      for (Method m : listener.getClass().getMethods()) {
         // loop through all valid method annotations
         for (Map.Entry<Class<? extends Annotation>, Class<?>[]> entry : allowedListeners.entrySet()) {
            Class<? extends Annotation> annotationType = entry.getKey();
            Class<?>[] eventTypes = entry.getValue();
            if (m.isAnnotationPresent(annotationType)) {
               testListenerMethodValidity(m, eventTypes, annotationType.getName());
               SecurityActions.setAccessible(m);
               ClientEventDispatcher.ClientListenerInvocation invocation = new ClientEventDispatcher.ClientListenerInvocation(listener, m);
               listenerMethodMap.computeIfAbsent(annotationType, a -> new ArrayList<>()).add(invocation);
            }
         }
      }

      return listenerMethodMap;
   }

   static void testListenerMethodValidity(Method m, Class<?>[] allowedParameters, String annotationName) {
      boolean isAllowed = false;
      for (Class<?> allowedParameter : allowedParameters) {
         if (m.getParameterCount() == 1 && m.getParameterTypes()[0].isAssignableFrom(allowedParameter)) {
            isAllowed = true;
            break;
         }
      }

      if (!isAllowed)
         throw HOTROD.incorrectClientListener(annotationName, Arrays.asList(allowedParameters));
      if (!m.getReturnType().equals(void.class))
         throw HOTROD.incorrectClientListener(annotationName);
   }

   @Override
   public void invokeEvent(ClientEvent clientEvent) {
      if (log.isTraceEnabled())
         log.tracef("Event %s received for listener with id=%s", clientEvent, Util.printArray(listenerId));

      switch (clientEvent.getType()) {
         case CLIENT_CACHE_ENTRY_CREATED:
            invokeCallbacks(clientEvent, ClientCacheEntryCreated.class);
            break;
         case CLIENT_CACHE_ENTRY_MODIFIED:
            invokeCallbacks(clientEvent, ClientCacheEntryModified.class);
            break;
         case CLIENT_CACHE_ENTRY_REMOVED:
            invokeCallbacks(clientEvent, ClientCacheEntryRemoved.class);
            break;
         case CLIENT_CACHE_ENTRY_EXPIRED:
            invokeCallbacks(clientEvent, ClientCacheEntryExpired.class);
            break;
      }
   }

   private void invokeCallbacks(ClientEvent event, Class<? extends Annotation> type) {
      List<ClientListenerInvocation> callbacks = invocables.get(type);
      if (callbacks != null) {
         for (ClientListenerInvocation callback : callbacks)
            callback.invoke(event);
      }
   }

   @Override
   public CompletableFuture<Void> executeFailover() {
      CompletableFuture<SocketAddress> future = op.copy().execute();
      if (remoteCache instanceof InvalidatedNearRemoteCache) {
         future = future.thenApply(socketAddress -> {
            ((InvalidatedNearRemoteCache<?, ?>) remoteCache).setBloomListenerAddress(socketAddress);
            return socketAddress;
         });
      }
      return future.thenApply(ignore -> null);
   }

   @Override
   protected void invokeFailoverEvent() {
      List<ClientListenerInvocation> callbacks = invocables.get(ClientCacheFailover.class);
      if (callbacks != null) {
         for (ClientListenerInvocation callback : callbacks) {
            callback.invoke(FAILOVER_EVENT_SINGLETON);
         }
      }
   }

   DataFormat getDataFormat() {
      return op.dataFormat();
   }

   static final class ClientListenerInvocation {
      final Object listener;
      final Method method;

      private ClientListenerInvocation(Object listener, Method method) {
         this.listener = listener;
         this.method = method;
      }

      public void invoke(ClientEvent event) {
         try {
            method.invoke(listener, event);
         } catch (Exception e) {
            throw HOTROD.exceptionInvokingListener(
                  e.getClass().getName(), method, listener, e);
         }
      }
   }
}
