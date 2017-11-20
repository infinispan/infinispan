package org.infinispan.client.hotrod.event;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.net.SocketException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryExpired;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientCacheFailover;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.AddClientListenerOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.Util;

/**
 * @author Galder Zamarreño
 */
public class ClientListenerNotifier {
   private static final Log log = LogFactory.getLog(ClientListenerNotifier.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final Map<Class<? extends Annotation>, Class<?>[]> allowedListeners = new HashMap<>(4);

   static {
      allowedListeners.put(ClientCacheEntryCreated.class, new Class[]{ClientCacheEntryCreatedEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheEntryModified.class, new Class[]{ClientCacheEntryModifiedEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheEntryRemoved.class, new Class[]{ClientCacheEntryRemovedEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheEntryExpired.class, new Class[]{ClientCacheEntryExpiredEvent.class, ClientCacheEntryCustomEvent.class});
      allowedListeners.put(ClientCacheFailover.class, new Class[]{ClientCacheFailoverEvent.class});
   }

   private final ConcurrentMap<WrappedByteArray, EventDispatcher> clientListeners = new ConcurrentHashMap<>();

   private final ExecutorService executor;
   private final Codec codec;
   private final Marshaller marshaller;
   private final TransportFactory transportFactory;

   private final Consumer<WrappedByteArray> failoverClientListener = this::failoverClientListener;
   private final List<String> whitelist;

   protected ClientListenerNotifier(
         ExecutorService executor, Codec codec,
         Marshaller marshaller, TransportFactory transportFactory, List<String> whitelist) {
      this.executor = executor;
      this.codec = codec;
      this.marshaller = marshaller;
      this.transportFactory = transportFactory;
      this.whitelist = whitelist;
   }

   public static ClientListenerNotifier create(Codec codec, Marshaller marshaller, TransportFactory transportFactory, List<String> whitelist) {
      ExecutorService executor = Executors.newCachedThreadPool(getRestoreThreadNameThreadFactory());
      return new ClientListenerNotifier(executor, codec, marshaller, transportFactory, whitelist);
   }

   public static ThreadFactory getRestoreThreadNameThreadFactory() {
      return r -> new Thread(() -> {
         final String originalName = Thread.currentThread().getName();
         try {
            r.run();
         } finally {
            Thread.currentThread().setName(originalName);
         }
      });
   }

   public Marshaller getMarshaller() {
      return marshaller;
   }

   public void addClientListener(AddClientListenerOperation op) {
      Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables = findMethods(op.listener);
      EventDispatcher eventDispatcher = new EventDispatcher(op, invocables, op.getCacheName());
      clientListeners.put(new WrappedByteArray(op.listenerId), eventDispatcher);
      if (trace)
         log.tracef("Add client listener with id %s, for listener %s and invocable methods %s",
               Util.printArray(op.listenerId), op.listener, invocables);
   }

   public void failoverClientListeners(Set<SocketAddress> failedServers) {
      // Compile all listener ids that need failing over
      List<WrappedByteArray> failoverListenerIds = new ArrayList<>();
      for (Map.Entry<WrappedByteArray, EventDispatcher> entry : clientListeners.entrySet()) {
         EventDispatcher dispatcher = entry.getValue();
         if (failedServers.contains(dispatcher.transport.getRemoteSocketAddress()))
            failoverListenerIds.add(entry.getKey());
      }
      if (trace && failoverListenerIds.isEmpty())
         log.tracef("No event listeners registered in faild servers: %s", failedServers);

      // Remove tracking listeners and read to the fallback transport
      failoverListenerIds.forEach(failoverClientListener);
   }

   public void failoverClientListener(byte[] listenerId) {
      failoverClientListener(new WrappedByteArray(listenerId));
   }

   private void failoverClientListener(WrappedByteArray listenerId) {
      EventDispatcher dispatcher = clientListeners.get(listenerId);
      removeClientListener(listenerId);
      // Invoke failover event callback, if presents
      invokeFailoverEvent(dispatcher);
      // Re-execute adding client listener in one of the remaining nodes
      dispatcher.op.execute();
      if (trace) {
         SocketAddress failedServerAddress = dispatcher.transport.getRemoteSocketAddress();
         log.tracef("Fallback listener id %s from a failed server %s to %s",
               Util.printArray(listenerId.getBytes()), failedServerAddress,
               dispatcher.op.getDedicatedTransport().getRemoteSocketAddress());
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
      EventDispatcher eventDispatcher = clientListeners.get(new WrappedByteArray(listenerId));
      executor.submit(eventDispatcher);
   }

   public void removeClientListener(byte[] listenerId) {
      removeClientListener(new WrappedByteArray(listenerId));
   }

   private void removeClientListener(WrappedByteArray listenerId) {
      EventDispatcher dispatcher = clientListeners.remove(listenerId);
      dispatcher.transport.release(); // force shutting it
      if (trace)
         log.tracef("Remove client listener with id %s", Util.printArray(listenerId.getBytes()));
   }

   public byte[] findListenerId(Object listener) {
      for (EventDispatcher dispatcher : clientListeners.values()) {
         if (dispatcher.op.listener.equals(listener))
            return dispatcher.op.listenerId;
      }
      return null;
   }

   public boolean isListenerConnected(byte[] listenerId) {
      EventDispatcher dispatcher = clientListeners.get(new WrappedByteArray(listenerId));
      // If listener not present, is not active
      return dispatcher != null && !dispatcher.stopped;
   }

   public Transport findTransport(byte[] listenerId) {
      EventDispatcher dispatcher = clientListeners.get(new WrappedByteArray(listenerId));
      if (dispatcher != null)
         return dispatcher.transport;

      return null;
   }

   public Map<Class<? extends Annotation>, List<ClientListenerInvocation>> findMethods(Object listener) {
      Map<Class<? extends Annotation>, List<ClientListenerInvocation>> listenerMethodMap = new HashMap<>(4, 0.99f);

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
                  invocables = new ArrayList<>();
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
      for (WrappedByteArray listenerId : clientListeners.keySet()) {
         if (trace)
            log.tracef("Remote cache manager stopping, remove client listener id %s", Util.printArray(listenerId.getBytes()));

         removeClientListener(listenerId);
      }

      executor.shutdown();
      try {
         executor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
   }

   public void invokeEvent(byte[] listenerId, ClientEvent clientEvent) {
      EventDispatcher eventDispatcher = clientListeners.get(new WrappedByteArray(listenerId));
      eventDispatcher.invokeClientEvent(clientEvent);
   }

   private final class EventDispatcher implements Runnable {
      final Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables;
      final AddClientListenerOperation op;
      final Transport transport;
      final String cacheName;
      volatile boolean stopped = false;

      private EventDispatcher(AddClientListenerOperation op,
            Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables,
            String cacheName) {
         this.op = op;
         this.transport = op.getDedicatedTransport();
         this.invocables = invocables;
         this.cacheName = cacheName;
      }

      @Override
      public void run() {
         Thread.currentThread().setName(getThreadName());
         while (!Thread.currentThread().isInterrupted()) {
            ClientEvent clientEvent = null;
            try {
               clientEvent = codec.readEvent(transport, op.listenerId, marshaller, whitelist);
               invokeClientEvent(clientEvent);
               // Nullify event, makes it easier to identify network vs invocation error messages
               clientEvent = null;
            } catch (TransportException e) {
               Throwable cause = e.getCause();
               if (cause instanceof ClosedChannelException || (cause instanceof SocketException && !transport.isValid())) {
                  // Channel closed, ignore and exit
                  log.debug("Channel closed, exiting event reader thread");
                  stopped = true;
                  return;
               } else if (cause instanceof SocketTimeoutException) {
                  log.debug("Timed out reading event, retry");
               } else if (clientEvent != null) {
                  log.unexpectedErrorConsumingEvent(clientEvent, e);
               } else if (cause instanceof IOException && cause.getMessage().contains("Connection reset by peer")) {
                  tryFailoverClientListener();
                  stopped = true;
                  return;
               } else {
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

      private void tryFailoverClientListener() {
         try {
            log.debug("Connection reset by peer, so failover client listener");
            failoverClientListener(op.listenerId);
         } catch (TransportException e) {
            log.debug("Unable to failover client listener, so ignore connection reset");
            try {
               transportFactory.addDisconnectedListener(() -> {
                  if (trace) {
                     log.tracef("Reconnecting client listener with id %s", Util.printArray(op.listenerId));
                  }
                  op.execute();
               });
            } catch (InterruptedException e1) {
               Thread.currentThread().interrupt();
            }
         }
      }

      String getThreadName() {
         String listenerId = Util.toHexString(op.listenerId, 8);
         return cacheName.isEmpty()
            ? "Client-Listener-" + listenerId
            : "Client-Listener-" + cacheName + "-" + listenerId;
      }

      void invokeClientEvent(ClientEvent clientEvent) {
         if (trace)
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
