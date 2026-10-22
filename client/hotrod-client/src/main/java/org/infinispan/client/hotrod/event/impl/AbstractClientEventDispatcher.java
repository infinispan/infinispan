package org.infinispan.client.hotrod.event.impl;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.Util;

/** Shared callback discovery, validation and delivery for cache listeners. */
public abstract class AbstractClientEventDispatcher extends EventDispatcher<ClientEvent> {

   private static final Map<Class<? extends Annotation>, Class<?>[]> allowedListeners = new HashMap<>(5);

   static {
      allowedListeners.put(ClientCacheEntryCreated.class, new Class[]{ClientCacheEntryCreatedEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheEntryModified.class, new Class[]{ClientCacheEntryModifiedEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheEntryRemoved.class, new Class[]{ClientCacheEntryRemovedEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheEntryExpired.class, new Class[]{ClientCacheEntryExpiredEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheFailover.class, new Class[]{ClientCacheFailoverEvent.class});
   }

   final Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables;

   final InternalRemoteCache<?, ?> remoteCache;

   AbstractClientEventDispatcher(Object listener, byte[] listenerId, InternalRemoteCache<?, ?> remoteCache,
                                 Runnable cleanup) {
      super(remoteCache.getName(), listener, listenerId, cleanup);
      this.remoteCache = remoteCache;
      this.invocables = findMethods(listener);
   }

   public static Map<Class<? extends Annotation>, List<ClientListenerInvocation>> findMethods(Object listener) {
      Map<Class<? extends Annotation>, List<ClientListenerInvocation>> listenerMethodMap = new HashMap<>(4, 0.99f);

      for (Method m : listener.getClass().getMethods()) {
         // loop through all valid method annotations
         for (Map.Entry<Class<? extends Annotation>, Class<?>[]> entry : allowedListeners.entrySet()) {
            Class<? extends Annotation> annotationType = entry.getKey();
            Class<?>[] eventTypes = entry.getValue();
            if (m.isAnnotationPresent(annotationType)) {
               testListenerMethodValidity(m, eventTypes, annotationType.getName());
               ReflectionUtil.setAccessible(m);
               ClientListenerInvocation invocation = new ClientListenerInvocation(listener, m);
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
      if (m.getReturnType() != void.class)
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

   final void invokeCallbacks(ClientEvent event, Class<? extends Annotation> type) {
      List<ClientListenerInvocation> callbacks = invocables.get(type);
      if (callbacks != null) {
         for (ClientListenerInvocation callback : callbacks)
            callback.invoke(event);
      }
   }

   DataFormat getDataFormat() {
      return remoteCache.getDataFormat();
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
