package org.infinispan.commons.util.concurrent;

import org.infinispan.assertions.ExceptionAssertion;
import org.infinispan.assertions.FutureAssertion;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.AssertJUnit.assertTrue;


/**
 * Tests notifications for {@link org.infinispan.commons.util.concurrent.NotifyingFuture}
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 * @author Sebastian Laskawiec
 */
@Test(groups = "functional", testName = "commons.NotifyingFutureTest")
public class NotifyingFutureTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(NotifyingFutureTest.class);

   public static final int FUTURE_GET_TIMEOUT_MS = 1000;

   public void testAttachingListenerBeforeSetFuture() throws Exception {
      //given
      final NotifyingFutureImpl<Integer> nf = createNotifyingFuture();
      final AtomicBoolean wasListenerInvoked = new AtomicBoolean(false);

      //when
      nf.attachListener(new FutureListener<Integer>() {
         @Override
         public void futureDone(Future<Integer> future) {
            wasListenerInvoked.set(true);
         }
      });

      Future<Integer> future = fork(new Callable<Integer>() {
         @Override
         public Integer call() throws Exception {
            // this invokes listeners, so there is no need to wait for them.
            nf.notifyDone(42);
            return 42;
         }
      });
      nf.setFuture(future);

      //ensure that all work is done.
      callGetAndExtractException(future);

      //then
      assertTrue(wasListenerInvoked.get());
      FutureAssertion.assertThat(nf, FUTURE_GET_TIMEOUT_MS).isDone().isNotCanceled().hasValue(42);
      FutureAssertion.assertThat(future, FUTURE_GET_TIMEOUT_MS).isDone().isNotCanceled().hasValue(42);
   }

   public void testCompletingJobBeforeListenerRegistration() throws Exception {
      //given
      final NotifyingFutureImpl<Integer> nf = createNotifyingFuture();
      final AtomicBoolean wasListenerInvoked = new AtomicBoolean(false);

      //when
      Future<Integer> future = fork(new Callable<Integer>() {
         @Override
         public Integer call() throws Exception {
            // this invokes listeners, so there is no need to wait for them.
            nf.notifyDone(42);
            return 42;
         }
      });
      nf.setFuture(future);
      //ensure that all work is done.
      callGetAndExtractException(future);

      nf.attachListener(new FutureListener<Integer>() {
         @Override
         public void futureDone(Future<Integer> future) {
            wasListenerInvoked.set(true);
         }
      });

      //then
      assertTrue(wasListenerInvoked.get());
      FutureAssertion.assertThat(nf, FUTURE_GET_TIMEOUT_MS).isDone().isNotCanceled().hasValue(42);
      FutureAssertion.assertThat(future, FUTURE_GET_TIMEOUT_MS).isDone().isNotCanceled().hasValue(42);
   }

   public void testForwardingExceptionFromInnerFuture() throws Exception {
      //given
      class TestingException extends Exception {
         TestingException(String message) {
            super(message);
         }
      }

      final NotifyingFutureImpl<Integer> nf = createNotifyingFuture();
      final AtomicReference<Exception> exceptionInListener = new AtomicReference<>();

      //when
      nf.attachListener(new FutureListener<Integer>() {
         @Override
         public void futureDone(Future<Integer> future) {
            exceptionInListener.set(callGetAndExtractException(future));
         }
      });

      Future<Integer> future = fork(new Callable<Integer>() {
         @Override
         public Integer call() throws Exception {
            TestingException testingException = new TestingException("Ignore me");
            nf.notifyException(testingException);
            throw testingException;
         }
      });
      nf.setFuture(future);
      //ensure that all work is done.
      callGetAndExtractException(future);

      //then
      ExceptionAssertion.assertThat(exceptionInListener.get()).IsNotNull().isTypeOf(ExecutionException.class)
            .hasCauseTypeOf(TestingException.class);
      FutureAssertion.assertThat(nf, FUTURE_GET_TIMEOUT_MS).isDone().isNotCanceled();
      FutureAssertion.assertThat(future, FUTURE_GET_TIMEOUT_MS).isDone().isNotCanceled();
   }

   private Exception callGetAndExtractException(Future<Integer> future) {
      Exception holder = null;
      try {
         future.get(FUTURE_GET_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
         holder = e;
      }
      return holder;
   }

   private <T> NotifyingFutureImpl<T> createNotifyingFuture() {
      return new NotifyingFutureImpl<>();
   }
}
