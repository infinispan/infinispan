package org.infinispan.client.hotrod.counter.impl;

import static org.infinispan.client.hotrod.impl.Util.await;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.counter.operation.AddListenerOperation;
import org.infinispan.client.hotrod.counter.operation.RemoveListenerOperation;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.event.impl.CounterEventDispatcher;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.concurrent.NonReentrantLock;
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
   private static final CompletableFuture<Short> NO_ERROR_FUTURE = CompletableFuture.completedFuture((short) HotRodConstants.NO_ERROR_STATUS);

   private final byte[] listenerId;
   private final ClientListenerNotifier notifier;
   private final CounterOperationFactory factory;
   private final ConcurrentMap<String, List<Consumer<HotRodCounterEvent>>> clientListeners = new ConcurrentHashMap<>();
   private final Lock lock = new NonReentrantLock();
   private volatile CounterEventDispatcher dispatcher;

   NotificationManager(ClientListenerNotifier notifier, CounterOperationFactory factory) {
      this.notifier = notifier;
      this.factory = factory;
      this.listenerId = new byte[16];
      ThreadLocalRandom.current().nextBytes(listenerId);
   }

   public <T extends CounterListener> Handle<T> addListener(String counterName, T listener) {
      if (trace) {
         log.tracef("Add listener for counter '%s'", counterName);
      }

      CounterEventDispatcher dispatcher = this.dispatcher;
      if (dispatcher != null) {
         return registerListener(counterName, listener, dispatcher.address());
      }
      log.debugf("ALock %s", lock);
      lock.lock();
      try {
         dispatcher = this.dispatcher;
         return registerListener(counterName, listener, dispatcher == null ? null : dispatcher.address());
      } finally {
         lock.unlock();
         log.debugf("AUnLock %s", lock);
      }
   }

   private <T extends CounterListener> Handle<T> registerListener(String counterName, T listener, SocketAddress address) {
      HandleImpl<T> handle = new HandleImpl<>(counterName, listener);
      clientListeners.computeIfAbsent(counterName, name -> {
         AddListenerOperation op = factory.newAddListenerOperation(counterName, listenerId, address);
         if (await(op.execute())) {
            if (address == null) {
               this.dispatcher = new CounterEventDispatcher(listenerId, clientListeners, op.getChannel().remoteAddress(), this::failover, op::cleanup);
               notifier.addDispatcher(dispatcher);
               notifier.startClientListener(listenerId);
            }
         }
         return new CopyOnWriteArrayList<>();
      }).add(handle);
      return handle;
   }

   private void removeListener(String counterName, HandleImpl<?> handle) {
      if (trace) {
         log.tracef("Remove listener for counter '%s'", counterName);
      }
      clientListeners.computeIfPresent(counterName, (name, list) -> {
         list.remove(handle);
         if (list.isEmpty()) {
            if (dispatcher != null) {
               RemoveListenerOperation op = factory.newRemoveListenerOperation(counterName, listenerId, dispatcher.address());
               if (!await(op.execute())) {
                  log.debugf("Failed to remove counter listener %s on server side", counterName);
               }
            }
            return null;
         }
         return list;
      });
   }

   private CompletableFuture<Short> failover() {
      dispatcher = null;
      Iterator<String> iterator = clientListeners.keySet().iterator();
      if (!iterator.hasNext()) {
         return NO_ERROR_FUTURE;
      }
      CompletableFuture<Short> cf = new CompletableFuture<>();
      String firstCounterName = iterator.next();
      AddListenerOperation op = factory.newAddListenerOperation(firstCounterName, listenerId, null);
      log.debugf("Lock %s", lock);
      lock.lock();
      if (dispatcher == null) {
         op.execute().whenComplete((useChannel, throwable) -> {
            if (throwable != null) {
               lock.unlock();
               log.debugf(throwable, "Failed to failover counter listener %s", firstCounterName);
               cf.completeExceptionally(throwable);
            } else {
               AtomicInteger counter = new AtomicInteger(1);
               SocketAddress address;
               try {
                  if (useChannel) {
                     log.debugf("Creating new counter event dispatcher on %s", op.getChannel());
                     dispatcher = new CounterEventDispatcher(listenerId, clientListeners, op.getChannel().remoteAddress(), this::failover, op::cleanup);
                     notifier.addDispatcher(dispatcher);
                     notifier.startClientListener(listenerId);
                  }
                  address = dispatcher.address();
               } catch (Throwable t) {
                  cf.completeExceptionally(t);
                  return;
               } finally {
                  lock.unlock();
                  log.debugf("UnLock %s", lock);
               }
               while (iterator.hasNext()) {
                  String counterName = iterator.next();
                  factory.newAddListenerOperation(counterName, listenerId, address).execute()
                        .whenComplete((useChannel2, throwable2) -> {
                           if (throwable2 != null) {
                              log.debugf(throwable2, "Failed to failover counter listener %s", counterName);
                              cf.completeExceptionally(throwable2);
                           } else {
                              if (useChannel2) {
                                 cf.completeExceptionally(new IllegalStateException("Unexpected to use another channel for the same counter"));
                              }
                              if (counter.decrementAndGet() == 0) {
                                 cf.complete((short) 0);
                              }
                           }
                        });
               }
               if (counter.decrementAndGet() == 0) {
                  cf.complete((short) 0);
               }
            }
         });
         return cf;
      } else {
         lock.unlock();
         log.debugf("UnLock %s", lock);
         return NO_ERROR_FUTURE;
      }
   }

   public void stop() {
      log.debugf("Stopping %s (%s)", this, lock);
      lock.lock();
      try {
         CompletableFuture[] futures = clientListeners.keySet().stream().map(counterName ->
               factory.newRemoveListenerOperation(counterName, listenerId, dispatcher.address()).execute())
               .toArray(CompletableFuture[]::new);
         await(CompletableFuture.allOf(futures));
         clientListeners.clear();
      } finally {
         lock.unlock();
      }
   }

   private class HandleImpl<T extends CounterListener> implements Handle<T>, Consumer<HotRodCounterEvent> {

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

      @Override
      public void accept(HotRodCounterEvent event) {
         try {
            listener.onUpdate(event);
         } catch (Throwable t) {
            log.debug("Exception in user listener", t);
         }
      }
   }
}
