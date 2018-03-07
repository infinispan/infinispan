package org.infinispan.counter;

import static java.lang.String.format;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.impl.BaseCounterTest;
import org.infinispan.counter.util.TestCounter;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * A notification test for the counters.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional")
public abstract class AbstractCounterNotificationTest extends BaseCounterTest {

   static final int CLUSTER_SIZE = 4;

   private static void incrementInEachCounter(TestCounter[] counters) {
      if (counters.length != CLUSTER_SIZE) {
         for (int i = 0; i < CLUSTER_SIZE; ++i) {
            counters[0].increment();
         }
      } else {
         for (TestCounter counter : counters) {
            counter.increment();
         }
      }
   }

   private static void decrementInEachCounter(TestCounter[] counters) {
      if (counters.length != CLUSTER_SIZE) {
         for (int i = 0; i < CLUSTER_SIZE; ++i) {
            counters[0].decrement();
         }
      } else {
         for (TestCounter counter : counters) {
            counter.decrement();
         }
      }
   }

   public void testSimpleListener(Method method) throws Exception {
      final String counterName = method.getName();
      final TestCounter[] counters = new TestCounter[clusterSize()];
      for (int i = 0; i < clusterSize(); ++i) {
         counters[i] = createCounter(counterManager(i), counterName);
      }

      Handle<ListenerQueue> l = counters[0].addListener(new ListenerQueue(counterName));

      incrementInEachCounter(counters);

      ListenerQueue lq = l.getCounterListener();
      printQueue(lq);
      lq.assertEvent(0, CounterState.VALID, 1, CounterState.VALID);
      lq.assertEvent(1, CounterState.VALID, 2, CounterState.VALID);
      lq.assertEvent(2, CounterState.VALID, 3, CounterState.VALID);
      lq.assertEvent(3, CounterState.VALID, 4, CounterState.VALID);

      assertEquals(4L, counters[0].getValue());

      l.remove();

      incrementInEachCounter(counters);

      assertTrue(l.getCounterListener().queue.isEmpty());
   }

   public void testMultipleListeners(Method method) throws InterruptedException {
      final String counterName = method.getName();
      final TestCounter[] counters = new TestCounter[clusterSize()];
      final List<Handle<ListenerQueue>> listeners = new ArrayList<>(clusterSize());
      for (int i = 0; i < clusterSize(); ++i) {
         counters[i] = createCounter(counterManager(i), counterName);
         listeners.add(counters[i].addListener(new ListenerQueue(counterName + "-listener-" + i)));
      }

      incrementInEachCounter(counters);

      for (int i = 0; i < clusterSize(); ++i) {
         ListenerQueue lq = listeners.get(i).getCounterListener();
         printQueue(lq);
         lq.assertEvent(0, CounterState.VALID, 1, CounterState.VALID);
         lq.assertEvent(1, CounterState.VALID, 2, CounterState.VALID);
         lq.assertEvent(2, CounterState.VALID, 3, CounterState.VALID);
         lq.assertEvent(3, CounterState.VALID, 4, CounterState.VALID);
         assertEquals(4L, counters[i].getValue());
      }
   }

   public void testExceptionInListener(Method method) throws InterruptedException {
      final String counterName = method.getName();
      final TestCounter[] counters = new TestCounter[clusterSize()];
      for (int i = 0; i < clusterSize(); ++i) {
         counters[i] = createCounter(counterManager(i), counterName);
      }

      counters[0].addListener(event -> {
         throw new RuntimeException("expected 1");
      });
      final Handle<ListenerQueue> l1 = counters[0].addListener(new ListenerQueue(counterName + "-listener-1"));
      counters[0].addListener(event -> {
         throw new RuntimeException("expected 2");
      });
      final Handle<ListenerQueue> l2 = counters[0].addListener(new ListenerQueue(counterName + "-listener-2"));

      incrementInEachCounter(counters);

      ListenerQueue lq = l1.getCounterListener();
      printQueue(lq);
      lq.assertEvent(0, CounterState.VALID, 1, CounterState.VALID);
      lq.assertEvent(1, CounterState.VALID, 2, CounterState.VALID);
      lq.assertEvent(2, CounterState.VALID, 3, CounterState.VALID);
      lq.assertEvent(3, CounterState.VALID, 4, CounterState.VALID);

      lq = l2.getCounterListener();
      printQueue(lq);
      lq.assertEvent(0, CounterState.VALID, 1, CounterState.VALID);
      lq.assertEvent(1, CounterState.VALID, 2, CounterState.VALID);
      lq.assertEvent(2, CounterState.VALID, 3, CounterState.VALID);
      lq.assertEvent(3, CounterState.VALID, 4, CounterState.VALID);

      assertEquals(4L, counters[0].getValue());

      decrementInEachCounter(counters);

      lq = l1.getCounterListener();
      printQueue(lq);
      lq.assertEvent(4, CounterState.VALID, 3, CounterState.VALID);
      lq.assertEvent(3, CounterState.VALID, 2, CounterState.VALID);
      lq.assertEvent(2, CounterState.VALID, 1, CounterState.VALID);
      lq.assertEvent(1, CounterState.VALID, 0, CounterState.VALID);

      lq = l2.getCounterListener();
      printQueue(lq);
      lq.assertEvent(4, CounterState.VALID, 3, CounterState.VALID);
      lq.assertEvent(3, CounterState.VALID, 2, CounterState.VALID);
      lq.assertEvent(2, CounterState.VALID, 1, CounterState.VALID);
      lq.assertEvent(1, CounterState.VALID, 0, CounterState.VALID);
   }

   @Override
   protected int clusterSize() {
      return CLUSTER_SIZE;
   }

   protected abstract TestCounter createCounter(CounterManager counterManager, String counterName);

   private void printQueue(ListenerQueue queue) {
      log.tracef("Queue is " + queue);
   }

   static class ListenerQueue implements CounterListener {

      private static final Log log = LogFactory.getLog(ListenerQueue.class);
      private static final boolean trace = log.isTraceEnabled();
      final BlockingQueue<CounterEvent> queue;
      final String name;

      ListenerQueue(String name) {
         queue = new LinkedBlockingQueue<>();
         this.name = name;
      }

      @Override
      public void onUpdate(CounterEvent event) {
         try {
            queue.put(event);
            if (trace) {
               log.tracef("[%s] add event %s", name, event);
            }
         } catch (InterruptedException e) {
            log.errorf(e, "[%s] interrupted while adding event %s", name, event);
         }
      }

      @Override
      public String toString() {
         return "ListenerQueue{" +
               "name=" + name +
               ", queue=" + queue +
               '}';
      }

      void assertEvent(long oldValue, CounterState oldState, long newValue, CounterState newState)
            throws InterruptedException {
         CounterEvent event = queue.poll(30, TimeUnit.SECONDS);
         assertNotNull("[" + name + "] Event not found", event);
         assertEquals(format("[%s] Wrong old value for event: %s.", name, event), oldValue, event.getOldValue());
         assertEquals(format("[%s] Wrong old state for event: %s.", name, event), oldState, event.getOldState());
         assertEquals(format("[%s] Wrong new value for event: %s.", name, event), newValue, event.getNewValue());
         assertEquals(format("[%s] Wrong new state for event: %s.", name, event), newState, event.getNewState());
      }
   }
}
