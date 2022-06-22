package org.infinispan.hotrod.event.impl;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.util.Util;
import org.infinispan.hotrod.event.ClientCacheFailoverEvent;
import org.infinispan.hotrod.event.ClientEvent;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.cache.InvalidatedNearRemoteCache;
import org.infinispan.hotrod.impl.cache.RemoteCache;
import org.infinispan.hotrod.impl.operations.ClientListenerOperation;

public final class ClientEventDispatcher extends EventDispatcher<ClientEvent> {

   public static final ClientCacheFailoverEvent FAILOVER_EVENT_SINGLETON = () -> ClientEvent.Type.CLIENT_CACHE_FAILOVER;

   private static final Map<Class<? extends Annotation>, Class<?>[]> allowedListeners = new HashMap<>(5);

   private final Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables;

   private final ClientListenerOperation op;
   private final RemoteCache<?, ?> remoteCache;

   ClientEventDispatcher(ClientListenerOperation op, SocketAddress address, Map<Class<? extends Annotation>,
         List<ClientListenerInvocation>> invocables, String cacheName, Runnable cleanup, RemoteCache<?, ?> remoteCache) {
      super(cacheName, op.listener, op.listenerId, address, cleanup);
      this.op = op;
      this.invocables = invocables;
      this.remoteCache = remoteCache;
   }

   public static ClientEventDispatcher create(ClientListenerOperation op, SocketAddress address, Runnable cleanup,
                                              RemoteCache<?, ?> remoteCache) {
      Map<Class<? extends Annotation>, List<ClientEventDispatcher.ClientListenerInvocation>> invocables = null;
      return new ClientEventDispatcher(op, address, invocables, op.getCacheName(), cleanup, remoteCache);
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
            //FIXME: invokeCallbacks(clientEvent, ClientCacheEntryCreated.class);
            break;
         case CLIENT_CACHE_ENTRY_MODIFIED:
           //FIXME: invokeCallbacks(clientEvent, ClientCacheEntryModified.class);
            break;
         case CLIENT_CACHE_ENTRY_REMOVED:
            //FIXME:invokeCallbacks(clientEvent, ClientCacheEntryRemoved.class);
            break;
         case CLIENT_CACHE_ENTRY_EXPIRED:
            //FIXME:invokeCallbacks(clientEvent, ClientCacheEntryExpired.class);
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
      CompletableFuture<SocketAddress> future = op.copy().execute().toCompletableFuture();
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
      List<ClientListenerInvocation> callbacks = null;//invocables.get(ClientCacheFailover.class);
      if (callbacks != null) {
         for (ClientListenerInvocation callback : callbacks) {
            callback.invoke(FAILOVER_EVENT_SINGLETON);
         }
      }
   }

   protected DataFormat getDataFormat() {
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
