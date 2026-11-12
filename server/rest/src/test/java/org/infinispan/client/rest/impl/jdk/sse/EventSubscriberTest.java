package org.infinispan.client.rest.impl.jdk.sse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Flow;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.rest.RestEventListener;
import org.infinispan.client.rest.RestResponseInfo;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.testing.ExceptionRunnable;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "client.rest.impl.jdk.sse.EventSubscriberTest")
public class EventSubscriberTest extends AbstractInfinispanTest {

   public void testCloseBeforeSubscribeCancelsLateSubscription() {
      RecordingListener listener = new RecordingListener();
      RecordingSubscription subscription = new RecordingSubscription();
      EventSubscriber subscriber = new EventSubscriber(listener);

      // The handle returned by listen() is closed before the response arrives.
      subscriber.close();
      assertTrue(listener.closed);

      subscriber.onSubscribe(subscription);

      assertTrue(subscription.cancelled);
      assertEquals(0, subscription.requested);
      assertFalse(listener.opened);
   }

   public void testCloseFromOnOpenCancelsSubscription() {
      RecordingListener listener = new RecordingListener();
      RecordingSubscription subscription = new RecordingSubscription();
      EventSubscriber subscriber = new EventSubscriber(listener);
      listener.onOpenAction = subscriber::close;

      subscriber.onSubscribe(subscription);

      assertTrue(listener.opened);
      assertTrue(subscription.cancelled);
      assertTrue(listener.closed);
   }

   public void testConcurrentCloseAndOpen() throws Exception {
      CheckPoint checkPoint = new CheckPoint();
      RecordingListener listener = new RecordingListener();
      RecordingSubscription subscription = new RecordingSubscription();
      EventSubscriber subscriber = new EventSubscriber(listener);

      // Hold the subscribing thread inside onOpen until the test lets it continue.
      listener.onOpenAction = () -> {
         checkPoint.trigger("opened");
         checkPoint.awaitStrict("continue", 10, TimeUnit.SECONDS);
      };
      Future<Void> subscribing = fork(() -> subscriber.onSubscribe(subscription));
      checkPoint.awaitStrict("opened", 10, TimeUnit.SECONDS);

      // Trigger a close while the subscribe is still running.
      assertFalse(subscribing.isDone());
      AtomicReference<Thread> closingThread = new AtomicReference<>();
      Future<Void> cancelling = fork(() -> {
         closingThread.set(Thread.currentThread());
         subscriber.close();
      });
      assertFalse(subscribing.isDone());
      assertFalse(cancelling.isDone());

      // Eventually, either the close completes or it gets blocked waiting for the intrinsic lock.
      eventually(() -> cancelling.isDone()
            || (closingThread.get() != null && closingThread.get().getState() == Thread.State.BLOCKED));

      // Release subscribing
      checkPoint.trigger("continue");

      // Allow it to proceed and verify that it indeed cancelled
      subscribing.get(10, TimeUnit.SECONDS);
      cancelling.get(10, TimeUnit.SECONDS);
      assertTrue(subscription.cancelled);
      assertTrue(listener.closed);
   }

   private static final class RecordingSubscription implements Flow.Subscription {
      private volatile long requested;
      private volatile boolean cancelled;

      @Override
      public void request(long n) {
         requested += n;
      }

      @Override
      public void cancel() {
         cancelled = true;
      }
   }

   private static final class RecordingListener implements RestEventListener {
      volatile boolean opened;
      volatile boolean closed;
      // Runs inside onOpen, on the thread delivering the subscription.
      volatile ExceptionRunnable onOpenAction;

      @Override
      public void onOpen(RestResponseInfo response) {
         opened = true;
         ExceptionRunnable action = onOpenAction;
         if (action != null) {
            try {
               action.run();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      }

      @Override
      public void close() {
         closed = true;
      }
   }
}
