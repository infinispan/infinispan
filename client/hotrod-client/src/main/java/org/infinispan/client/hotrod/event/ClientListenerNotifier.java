package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.Util;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

/**
 * @author Galder Zamarreño
 */
public class ClientListenerNotifier {
   private static final Log log = LogFactory.getLog(ClientListenerNotifier.class, Log.class);

   private static final Map<Class<? extends Annotation>, Class<?>[]> allowedListeners =
         new HashMap<Class<? extends Annotation>, Class<?>[]>(4);

   static {
      allowedListeners.put(ClientCacheEntryCreated.class, new Class[]{ClientCacheEntryCreatedEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheEntryModified.class, new Class[]{ClientCacheEntryModifiedEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheEntryRemoved.class, new Class[]{ClientCacheEntryRemovedEvent.class, ClientCacheEntryCustomEvent.class});
   }

   private final ConcurrentMap<byte[], EventDispatcher> clientListeners = CollectionFactory.makeConcurrentMap(
         ByteArrayEquivalence.INSTANCE, AnyEquivalence.getInstance());

   private final ExecutorService executor;
   private final Codec codec;
   private final Marshaller marshaller;

   public ClientListenerNotifier(ExecutorService executor, Codec codec, Marshaller marshaller) {
      this.executor = executor;
      this.codec = codec;
      this.marshaller = marshaller;
   }

   public void addClientListener(byte[] listenerId, Object listener, Transport transport) {
      Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables = findMethods(listener);
      // ... TODO ???
      // transport.disableSocketTimeout();
      EventDispatcher eventDispatcher = new EventDispatcher(listenerId, listener, invocables, transport, marshaller);
      clientListeners.put(listenerId, eventDispatcher);
      executor.submit(eventDispatcher);
   }

   public void removeClientListener(byte[] listenerId) {
      EventDispatcher dispatcher = clientListeners.get(listenerId);
      dispatcher.transport.release(); // force shutting it
      clientListeners.remove(listenerId);
   }

   public byte[] findListenerId(Object listener) {
      for (EventDispatcher dispatcher : clientListeners.values()) {
         if (dispatcher.listener.equals(listener))
            return dispatcher.listenerId;
      }
      return null;
   }

   public Transport findTransport(byte[] listenerId) {
      EventDispatcher dispatcher = clientListeners.get(listenerId);
      if (dispatcher != null)
         return dispatcher.transport;

      return null;
   }

   // TODO: Similar code in AbstractListenerImpl...
   private Map<Class<? extends Annotation>, List<ClientListenerInvocation>> findMethods(Object listener) {
      Map<Class<? extends Annotation>, List<ClientListenerInvocation>> listenerMethodMap =
            new HashMap<Class<? extends Annotation>, List<ClientListenerInvocation>>(4, 0.99f);

      for (Method m : listener.getClass().getMethods()) {
         // loop through all valid method annotations
         for (Map.Entry<Class<? extends Annotation>, Class<?>[]> entry : allowedListeners.entrySet()) {
            Class<? extends Annotation> annotationType = entry.getKey();
            Class<?>[] eventTypes = entry.getValue();
            if (m.isAnnotationPresent(annotationType)) {
               testListenerMethodValidity(m, eventTypes, annotationType.getName());
               m.setAccessible(true);
               ClientListenerInvocation invocation = new ClientListenerInvocation(listener, m);
               List<ClientListenerInvocation> invocables = listenerMethodMap.get(annotationType);
               if (invocables == null) {
                  invocables = new ArrayList<ClientListenerInvocation>();
                  listenerMethodMap.put(annotationType, invocables);
               }

               invocables.add(invocation);
            }
         }
      }

      return listenerMethodMap;
   }

   private void testListenerMethodValidity(Method m, Class<?>[] allowedParameters, String annotationName) {
      boolean isAllowed = false;
      for (Class<?> allowedParameter : allowedParameters) {
         if (m.getParameterTypes().length == 1 && m.getParameterTypes()[0].isAssignableFrom(allowedParameter)) {
            isAllowed = true;
            break;
         }
      }

      if (!isAllowed)
         throw log.incorrectClientListener(annotationName, Arrays.asList(allowedParameters));
      if (!m.getReturnType().equals(void.class))
         throw log.incorrectClientListener(annotationName);
   }

   private final class EventDispatcher implements Runnable {
      final byte[] listenerId;
      final Object listener;
      final Transport transport;
      final Marshaller marshaller;
      final Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables;

      private EventDispatcher(byte[] listenerId, Object listener,
            Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables,
            Transport transport, Marshaller marshaller) {
         this.listenerId = listenerId;
         this.listener = listener;
         this.transport = transport;
         this.marshaller = marshaller;
         this.invocables = invocables;
      }

      @Override
      public void run() {
         try {
            while (true) {
               ClientEvent clientEvent = null;
               try {
                  clientEvent = codec.readEvent(transport, listenerId, marshaller);
                  // transport.setBlocking(true);
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
                  }

                  // Nullify event, makes it easier to identify network vs invocation error messages
                  clientEvent = null;
               } catch (TransportException e) {
                  if (e.getCause() instanceof ClosedChannelException)
                     throw (ClosedChannelException) e.getCause();
                  else if (clientEvent != null)
                     log.unexpectedErrorConsumingEvent(clientEvent, e);
                  else
                     log.unableToReadEventFromServer(e);
               } catch (Throwable t) {
                  // transport.setBlocking(true);
                  if (clientEvent != null)
                     log.unexpectedErrorConsumingEvent(clientEvent, t);
                  else
                     log.unableToReadEventFromServer(t);
               }
            }
         } catch (ClosedChannelException e) {
            // Channel closed, ignore and exit
         }
      }

      private void invokeCallbacks(ClientEvent event, Class<? extends Annotation> type) {
         List<ClientListenerInvocation> callbacks = invocables.get(type);
         if (callbacks != null) {
            for (ClientListenerInvocation callback : callbacks)
               callback.invoke(event);
         }
      }
   }

   private static final class ClientListenerInvocation {
      private static final Log log = LogFactory.getLog(ClientListenerInvocation.class, Log.class);

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
            throw log.exceptionInvokingListener(
                  e.getClass().getName(), method, listener, e);
         }
      }
   }
}
