package org.infinispan.client.hotrod.counter;

import static org.infinispan.server.hotrod.counter.impl.BaseCounterImplTest.assertNextValidEvent;
import static org.infinispan.server.hotrod.counter.impl.BaseCounterImplTest.assertNoEvents;
import static org.infinispan.server.hotrod.counter.impl.BaseCounterImplTest.assertValidEvent;
import static org.infinispan.test.TestingUtil.extractField;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.counter.impl.ConnectionManager;
import org.infinispan.client.hotrod.counter.impl.NotificationManager;
import org.infinispan.client.hotrod.counter.impl.RemoteCounterManager;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.server.hotrod.counter.impl.BaseCounterImplTest;
import org.testng.annotations.Test;

/**
 * A base test class for {@link org.infinispan.counter.api.StrongCounter} and {@link
 * org.infinispan.counter.api.WeakCounter} implementation in the Hot Rod client.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@Test(groups = "functional")
public abstract class BaseCounterAPITest<T> extends AbstractCounterTest {

   private static final CounterListener EXCEPTION_LISTENER = entry -> {
      throw new RuntimeException("induced");
   };

   public void testExceptionInListener(Method method) throws InterruptedException {
      final String counterName = method.getName();
      T counter = defineAndCreateCounter(counterName, 0);

      Handle<BaseCounterImplTest.EventLogger> handle1 = addListenerTo(counter, new BaseCounterImplTest.EventLogger());
      Handle<CounterListener> handleEx = addListenerTo(counter, EXCEPTION_LISTENER);
      Handle<BaseCounterImplTest.EventLogger> handle2 = addListenerTo(counter, new BaseCounterImplTest.EventLogger());

      add(counter, 1, 1);
      add(counter, -1, 0);
      add(counter, 10, 10);
      add(counter, 1, 11);
      add(counter, 2, 13);

      assertNextValidEvent(handle1, 0, 1);
      assertNextValidEvent(handle1, 1, 0);
      assertNextValidEvent(handle1, 0, 10);
      assertNextValidEvent(handle1, 10, 11);
      assertNextValidEvent(handle1, 11, 13);

      assertNextValidEvent(handle2, 0, 1);
      assertNextValidEvent(handle2, 1, 0);
      assertNextValidEvent(handle2, 0, 10);
      assertNextValidEvent(handle2, 10, 11);
      assertNextValidEvent(handle2, 11, 13);


      assertNoEvents(handle1);
      assertNoEvents(handle2);

      handle1.remove();
      handle2.remove();
      handleEx.remove();
   }

   public void testConcurrentListenerAddAndRemove(Method method) throws InterruptedException, ExecutionException {
      String counterName = method.getName();
      defineAndCreateCounter(counterName, 1);
      List<T> counters = getCounters(counterName);

      List<IncrementTask> taskList = counters.stream()
            .map(IncrementTask::new)
            .collect(Collectors.toList());

      List<Future<?>> futureTaskList = taskList.stream()
            .map(this::fork)
            .collect(Collectors.toList());

      T counter = counters.get(0);

      Handle<BaseCounterImplTest.EventLogger> handle = addListenerTo(counter, new BaseCounterImplTest.EventLogger());
      //lets wait for at least some events...
      eventually(() -> handle.getCounterListener().size() > 5);
      handle.remove();

      taskList.forEach(IncrementTask::stop);
      futureTaskList.forEach(this::awaitFuture);

      drainAndCheckEvents(handle);

      assertNoEvents(handle);

      increment(counter);

      assertNoEvents(handle);
   }

   public void testListenerFailover(Method method) throws Exception {
      String counterName = method.getName();
      T counter = defineAndCreateCounter(counterName, 2);
      Handle<BaseCounterImplTest.EventLogger> handle = addListenerTo(counter, new BaseCounterImplTest.EventLogger());
      add(counter, 1, 3);
      assertNextValidEvent(handle, 2, 3);

      InetSocketAddress eventAddress = findEventServer();
      int killIndex = -1;
      for (int i = 0; i < servers.size(); ++i) {
         if (servers.get(i).getAddress().getPort() == eventAddress.getPort()) {
            killIndex = i;
            break;
         }
      }

      assert killIndex != -1;

      killServer(killIndex);

      add(counter, 1, 4);
      add(counter, 1, 5);

      //sometimes, it takes some time to reconnect.
      //In any case, the first operation triggers a new topology and it should be reconnect after it!
      CounterEvent event = handle.getCounterListener().waitingPoll();
      if (event.getOldValue() == 3) {
         assertValidEvent(event, 3, 4);
         assertNextValidEvent(handle, 4, 5);
      } else {
         assertValidEvent(event, 4, 5);
      }

      handle.remove();
   }

   abstract void increment(T counter);

   abstract void add(T counter, long delta, long result);

   abstract T defineAndCreateCounter(String counterName, long initialValue);

   abstract <L extends CounterListener> Handle<L> addListenerTo(T counter, L logger);

   abstract List<T> getCounters(String name);

   private InetSocketAddress findEventServer() throws InvocationTargetException, IllegalAccessException {
      Object notificationManager = extractField(RemoteCounterManager.class, counterManager(), "notificationManager");
      Object connectionManager = extractField(NotificationManager.class, notificationManager, "connectionManager");
      Method method = ReflectionUtil.findMethod(ConnectionManager.class, "getServerInUse");
      method.setAccessible(true);
      return (InetSocketAddress) method.invoke(connectionManager);
   }

   private void awaitFuture(Future<?> future) {
      try {
         future.get();
      } catch (InterruptedException e) {
         //no-op
      } catch (ExecutionException e) {
         throw new RuntimeException(e);
      }
   }

   private void drainAndCheckEvents(Handle<BaseCounterImplTest.EventLogger> handle) throws InterruptedException {
      //we don't know how many but they must be ordered.
      //at least, one event!
      CounterEvent event = handle.getCounterListener().waitingPoll();
      log.tracef("First Event=%s", event);
      long preValue = event.getOldValue();
      assertValidEvent(event, preValue, ++preValue);
      while ((event = handle.getCounterListener().poll()) != null) {
         log.tracef("Next Event=%s", event);
         assertValidEvent(event, preValue, ++preValue);
      }
   }


   private class IncrementTask implements Runnable {

      private final T counter;
      private volatile boolean run;

      private IncrementTask(T counter) {
         this.counter = counter;
         this.run = true;
      }

      @Override
      public void run() {
         while (run) {
            increment(counter);
         }
      }

      void stop() {
         run = false;
      }
   }

}
