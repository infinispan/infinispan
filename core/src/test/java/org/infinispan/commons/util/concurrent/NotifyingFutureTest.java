package org.infinispan.commons.util.concurrent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "commons.NotifyingFutureTest")
public class NotifyingFutureTest {

   public void testDoneThisThread() throws ExecutionException, InterruptedException {
      testDone(new WithinThreadExecutor(), 0, 0);
   }

   public void testExceptionThisThread() throws ExecutionException, InterruptedException {
      testException(new WithinThreadExecutor(), 0, 0);
   }

   public void testDoneOtherThread1() throws ExecutionException, InterruptedException {
      testDoneOtherThread(100, 0);
   }

   @Test(groups = "unstable", description = "See ISPN-4029")
   public void testDoneOtherThread2() throws ExecutionException, InterruptedException {
      testDoneOtherThread(0, 100);
   }

   @Test(groups = "unstable", description = "See ISPN-4029")
   public void testExceptionOtherThread1() throws ExecutionException, InterruptedException {
      testExceptionOtherThread(100, 0);
   }

   @Test(groups = "unstable", description = "See ISPN-4029")
   public void testExceptionOtherThread2() throws ExecutionException, InterruptedException {
      testExceptionOtherThread(0, 100);
   }

   private void testDoneOtherThread(long beforeSetDelay, long beforeExecuteDelay) throws ExecutionException, InterruptedException {
      ThreadPoolExecutor tpe = null;
      try {
         tpe = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1));
         testDone(tpe, beforeSetDelay, beforeExecuteDelay);
      } finally {
         if (tpe != null) tpe.shutdown();
      }
   }

   private void testExceptionOtherThread(long beforeSetDelay, long beforeExecuteDelay) throws ExecutionException, InterruptedException {
      ThreadPoolExecutor tpe = null;
      try {
         tpe = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1));
         testException(tpe, beforeSetDelay, beforeExecuteDelay);
      } finally {
         if (tpe != null) tpe.shutdown();
      }
   }

   private void testDone(ExecutorService service, long beforeSetDelay, final long beforeExecuteDelay) throws InterruptedException, ExecutionException {
      final NotifyingFutureImpl<Integer> nf = new NotifyingFutureImpl<Integer>();
      Future<Integer> f = service.submit(new Callable<Integer>() {
         @Override
         public Integer call() throws Exception {
            Thread.sleep(beforeExecuteDelay);
            int retval = 42;
            nf.notifyDone(retval);
            return retval;
         }
      });
      try {
         assertEquals(Integer.valueOf(42), f.get(0, TimeUnit.NANOSECONDS));
      } catch (TimeoutException e) {
      }
      Thread.sleep(beforeSetDelay);
      nf.setFuture(f);
      try {
         assertEquals(Integer.valueOf(42), f.get(0, TimeUnit.NANOSECONDS));
      } catch (TimeoutException e) {
      }
      final AtomicInteger retval = new AtomicInteger(-1);
      final CountDownLatch latch = new CountDownLatch(1);
      nf.attachListener(new FutureListener<Integer>() {
         @Override
         public void futureDone(Future<Integer> future) {
            try {
               retval.set(future.get());
            } catch (Exception e) {
               e.printStackTrace();
            } finally {
               latch.countDown();
            }
         }
      });
      latch.await();
      assertTrue(nf.isDone());
      assertFalse(nf.isCancelled());
      assertTrue(f.isDone());
      assertFalse(f.isCancelled());
      assertEquals(42, retval.get());
      assertEquals(Integer.valueOf(42), nf.get());
      assertEquals(Integer.valueOf(42), f.get());
   }

   private void testException(ExecutorService service, long beforeSetDelay, final long beforeExecuteDelay) throws InterruptedException, ExecutionException {
      final NotifyingFutureImpl<Integer> nf = new NotifyingFutureImpl<Integer>();
      Future<Integer> f = service.submit(new Callable<Integer>() {
         @Override
         public Integer call() throws Exception {
            Thread.sleep(beforeExecuteDelay);
            Exception e = new IllegalStateException();
            nf.notifyException(e);
            throw e;
         }
      });
      Thread.sleep(beforeSetDelay);
      nf.setFuture(f);
      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<Throwable> ex = new AtomicReference<Throwable>(null);
      nf.attachListener(new FutureListener<Integer>() {
         @Override
         public void futureDone(Future<Integer> future) {
            try {
               future.get();
            } catch (Throwable t) {
               ex.set(t);
            } finally {
               latch.countDown();
            }
         }
      });
      if (!latch.await(5, TimeUnit.SECONDS)) {
         fail("Not finished withing time limit (5 seconds)");
      }
      assertTrue(nf.isDone());
      assertFalse(nf.isCancelled());
      assertTrue(f.isDone());
      assertFalse(f.isCancelled());
      assertTrue(ex.get() instanceof ExecutionException);
      assertTrue(ex.get().getCause() instanceof IllegalStateException);
      boolean thrown = false;
      try {
         nf.get();
      } catch (ExecutionException e) {
         assertTrue(e instanceof ExecutionException);
         assertTrue(e.getCause() instanceof IllegalStateException);
         thrown = true;
      }
      assertTrue(thrown);
   }
}
