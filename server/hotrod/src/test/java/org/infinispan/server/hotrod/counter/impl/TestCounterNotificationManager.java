package org.infinispan.server.hotrod.counter.impl;

import static org.infinispan.server.hotrod.counter.op.CounterListenerOp.createListener;
import static org.infinispan.server.hotrod.counter.op.CounterListenerOp.removeListener;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.exception.CounterException;
import org.infinispan.server.hotrod.counter.op.CounterListenerOp;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestResponse;

/**
 * A test client notification manager.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class TestCounterNotificationManager {

   private final WrappedByteArray listenerId;
   private final Map<String, List<UserListener<?>>> userListenerList;
   private final HotRodClient client;

   TestCounterNotificationManager(HotRodClient client) {
      this.client = client;
      byte[] listenerId = new byte[16];
      ThreadLocalRandom.current().nextBytes(listenerId);
      this.listenerId = new WrappedByteArray(listenerId);
      userListenerList = new ConcurrentHashMap<>();
   }

   public WrappedByteArray getListenerId() {
      return listenerId;
   }

   public void accept(TestCounterEventResponse event) {
      List<UserListener<?>> list = userListenerList.get(event.getCounterName());
      list.parallelStream().forEach(userListener -> userListener.trigger(event.getCounterEvent()));
   }

   public <T extends CounterListener> Handle<T> register(String counterName, T listener) {
      UserListener<T> ul = new UserListener<>(listener, userListener -> remove(counterName, userListener));
      userListenerList.compute(counterName, (s, userListeners) -> add(s, userListeners, ul));
      return ul;
   }

   public void start() {
      client.registerCounterNotificationManager(this);
   }

   private List<UserListener<?>> add(String counterName, List<UserListener<?>> list, UserListener<?> listener) {
      if (list == null) {
         CounterListenerOp op = createListener(client.protocolVersion(), counterName, listenerId.getBytes());
         client.writeOp(op);
         TestResponse response = client.getResponse(op);
         switch (response.getStatus()) {
            case Success:
               break;
            case OperationNotExecuted:
               break;
            case KeyDoesNotExist:
               throw new CounterException("Counter " + counterName + " doesn't exist");
            default:
               throw new IllegalStateException("Unknown status " + response.getStatus());
         }
         list = new CopyOnWriteArrayList<>();
      }
      list.add(listener);
      return list;
   }

   private void remove(String counterName, UserListener<?> listener) {
      userListenerList.computeIfPresent(counterName, (name, list) -> {
         list.remove(listener);
         if (list.isEmpty()) {
            CounterListenerOp op = removeListener(client.protocolVersion(), counterName, listenerId.getBytes());
            client.writeOp(op);
            TestResponse response = client.getResponse(op);
            switch (response.getStatus()) {
               case Success:
                  break;
               case OperationNotExecuted:
                  break;
               case KeyDoesNotExist:
                  throw new CounterException("Counter " + counterName + " doesn't exist");
               default:
                  throw new IllegalStateException("Unknown status " + response.getStatus());
            }
            return null;
         }
         return list;
      });
   }

   private static class UserListener<T extends CounterListener> implements Handle<T> {

      private final T listener;
      private final Consumer<UserListener<?>> removeConsumer;

      private UserListener(T listener,
            Consumer<UserListener<?>> removeConsumer) {
         this.listener = listener;
         this.removeConsumer = removeConsumer;
      }

      @Override
      public T getCounterListener() {
         return listener;
      }

      @Override
      public void remove() {
         removeConsumer.accept(this);
      }

      void trigger(CounterEvent event) {
         try {
            listener.onUpdate(event);
         } catch (Exception e) {
            //ignored
         }
      }
   }
}
