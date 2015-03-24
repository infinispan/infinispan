package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientCacheFailover;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.AddClientListenerOperation;
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
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
      allowedListeners.put(ClientCacheFailover.class, new Class[]{ClientCacheFailoverEvent.class});
   }

   private final ConcurrentMap<byte[], EventDispatcher> clientListeners = CollectionFactory.makeConcurrentMap(
         ByteArrayEquivalence.INSTANCE, AnyEquivalence.getInstance());

   private final ExecutorService executor;
   private final Codec codec;
   private final Marshaller marshaller;

   protected ClientListenerNotifier(ExecutorService executor, Codec codec, Marshaller marshaller) {
      this.executor = executor;
      this.codec = codec;
      this.marshaller = marshaller;
   }

   public static ClientListenerNotifier create(Codec codec, Marshaller marshaller) {
      return new ClientListenerNotifier(Executors.newCachedThreadPool(), codec, marshaller);
   }

   public Marshaller getMarshaller() {
      return marshaller;
   }

   public void addClientListener(AddClientListenerOperation op) {
      Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables = findMethods(op.listener);
      EventDispatcher eventDispatcher = new EventDispatcher(op, invocables);
      clientListeners.put(op.listenerId, eventDispatcher);
      if (log.isTraceEnabled())
         log.tracef("Add client listener with id %s, for listener %s and invocable methods %s",
               Util.printArray(op.listenerId), op.listener, invocables);
   }

   public void failoverClientListeners(Set<SocketAddress> failedServers) {
      // Compile all listener ids that need failing over
      List<byte[]> failoverListenerIds = new ArrayList<>();
      for (Map.Entry<byte[], EventDispatcher> entry : clientListeners.entrySet()) {
         EventDispatcher dispatcher = entry.getValue();
         if (failedServers.contains(dispatcher.transport.getRemoteSocketAddress()))
            failoverListenerIds.add(entry.getKey());
      }
      if (log.isTraceEnabled() && failoverListenerIds.isEmpty())
         log.tracef("No event listeners registered in faild servers: %s", failedServers);

      // Remove tracking listeners and read to the fallback transport
      for (byte[] listenerId : failoverListenerIds) {
         EventDispatcher dispatcher = clientListeners.get(listenerId);
         removeClientListener(listenerId);
         // Invoke failover event callback, if presents
         invokeFailoverEvent(dispatcher);
         // Re-execute adding client listener in one of the remaining nodes
         dispatcher.op.execute();
         if (log.isTraceEnabled()) {
            SocketAddress failedServerAddress = dispatcher.transport.getRemoteSocketAddress();
            log.tracef("Fallback listener id %s from a failed server %s to %s",
                  Util.printArray(listenerId), failedServerAddress,
                  dispatcher.op.getDedicatedTransport().getRemoteSocketAddress());
         }
      }
   }

   private void invokeFailoverEvent(EventDispatcher dispatcher) {
      List<ClientListenerInvocation> callbacks = dispatcher.invocables.get(ClientCacheFailover.class);
      if (callbacks != null) {
         for (ClientListenerInvocation callback : callbacks)
            callback.invoke(ClientEvents.mkCachefailoverEvent());
      }
   }

   public void startClientListener(byte[] listenerId) {
      EventDispatcher eventDispatcher = clientListeners.get(listenerId);
      executor.submit(eventDispatcher);
   }

   public void removeClientListener(byte[] listenerId) {
      EventDispatcher dispatcher = clientListeners.remove(listenerId);
      dispatcher.transport.release(); // force shutting it
      if (log.isTraceEnabled())
         log.tracef("Remove client listener with id %s", Util.printArray(listenerId));
   }

   public byte[] findListenerId(Object listener) {
      for (EventDispatcher dispatcher : clientListeners.values()) {
         if (dispatcher.op.listener.equals(listener))
            return dispatcher.op.listenerId;
      }
      return null;
   }

   public boolean isListenerConnected(byte[] listenerId) {
      EventDispatcher dispatcher = clientListeners.get(listenerId);
      // If listener not present, is not active
      return dispatcher != null && !dispatcher.stopped;
   }

   public Transport findTransport(byte[] listenerId) {
      EventDispatcher dispatcher = clientListeners.get(listenerId);
      if (dispatcher != null)
         return dispatcher.transport;

      return null;
   }

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
               SecurityActions.setAccessible(m);
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

   public Set<Object> getListeners(byte[] cacheName) {
      Set<Object> ret = new HashSet<>(clientListeners.size());
      for (EventDispatcher dispatcher : clientListeners.values()) {
         if (Arrays.equals(dispatcher.op.cacheName, cacheName))
            ret.add(dispatcher.op.listener);
      }

      return ret;
   }

   public void stop() {
      for (byte[] listenerId : clientListeners.keySet()) {
         if (log.isTraceEnabled())
            log.tracef("Remote cache manager stopping, remove client listener id %s", Util.printArray(listenerId));

         removeClientListener(listenerId);
      }
   }

   public void invokeEvent(byte[] listenerId, ClientEvent clientEvent) {
      EventDispatcher eventDispatcher = clientListeners.get(listenerId);
      eventDispatcher.invokeClientEvent(clientEvent);
   }

   private final class EventDispatcher implements Runnable {
      final Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables;
      final AddClientListenerOperation op;
      final Transport transport;
      volatile boolean stopped = false;

      private EventDispatcher(AddClientListenerOperation op,
            Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables) {
         this.op = op;
         this.transport = op.getDedicatedTransport();
         this.invocables = invocables;
      }

      @Override
      public void run() {
         Thread.currentThread().setName("Client-Listener-" + Util.toHexString(op.listenerId, 8));
         while (!Thread.currentThread().isInterrupted()) {
            ClientEvent clientEvent = null;
            try {
               clientEvent = codec.readEvent(transport, op.listenerId, marshaller);
               invokeClientEvent(clientEvent);
               // Nullify event, makes it easier to identify network vs invocation error messages
               clientEvent = null;
            } catch (TransportException e) {
               Throwable cause = e.getCause();
               if (cause instanceof ClosedChannelException) {
                  // Channel closed, ignore and exit
                  log.debug("Channel closed, exiting event reader thread");
                  stopped = true;
                  return;
               } else if (cause instanceof SocketTimeoutException) {
                  log.debug("Timed out reading event, retry");
               } else if (clientEvent != null) {
                  log.unexpectedErrorConsumingEvent(clientEvent, e);
               }  else {
                  log.unrecoverableErrorReadingEvent(e, transport.getRemoteSocketAddress());
                  stopped = true;
                  return; // Server is likely gone!
               }
            } catch (CancelledKeyException e) {
               // Cancelled key exceptions are also thrown when the channel has been closed
               log.debug("Key cancelled, most likely channel closed, exiting event reader thread");
               stopped = true;
               return;
            } catch (Throwable t) {
               if (clientEvent != null)
                  log.unexpectedErrorConsumingEvent(clientEvent, t);
               else
                  log.unableToReadEventFromServer(t, transport.getRemoteSocketAddress());
               if (!transport.isValid()) {
                  stopped = true;
                  return;
               }
            }
         }
      }

      void invokeClientEvent(ClientEvent clientEvent) {
         if (log.isTraceEnabled())
            log.tracef("Event %s received for listener with id=%s", clientEvent, Util.printArray(op.listenerId));

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
