package org.infinispan.executors;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "executors.ExecutorAllCompletionServiceTest")
public class ExecutorAllCompletionServiceTest extends AbstractInfinispanTest {

   private ExecutorService lastExecutorService;

   @AfterClass(alwaysRun = true)
   public void stopExecutors() {
      if (lastExecutorService != null) {
         lastExecutorService.shutdownNow();
      }
   }

   public void testWaitForAll() {
      ExecutorAllCompletionService service = createService(1);
      long before = System.currentTimeMillis();
      service.submit(new WaitRunnable(500), null);
      service.submit(new WaitRunnable(500), null);
      service.waitUntilAllCompleted();
      long after = System.currentTimeMillis();
      assertTrue(after - before >= 1000);
      assertTrue(service.isAllCompleted());
      assertFalse(service.isExceptionThrown());
   }

   public void testExceptions() {
      ExecutorAllCompletionService service = createService(1);
      service.submit(new WaitRunnable(1), null);
      service.submit(new ExceptionRunnable("second"), null);
      service.submit(new WaitRunnable(1), null);
      service.submit(new ExceptionRunnable("third"), null);
      service.waitUntilAllCompleted();
      assertTrue(service.isAllCompleted());
      assertTrue(service.isExceptionThrown());
      assertEquals("second", findCause(service.getFirstException()).getMessage());
   }

   public void testParallelWait() throws InterruptedException {
      final ExecutorAllCompletionService service = createService(2);
      for (int i = 0; i < 300; ++i) {
         service.submit(new WaitRunnable(10), null);
      }
      List<Thread> threads = new ArrayList<>(10);
      for (int i = 0; i < 10; ++i) {
         Thread t = new Thread(() -> {
            service.waitUntilAllCompleted();
            assertTrue(service.isAllCompleted());
            assertFalse(service.isExceptionThrown());
         });
         threads.add(t);
         t.start();
      }
      for (Thread t : threads) {
         t.join();
      }
      assertTrue(service.isAllCompleted());
      assertFalse(service.isExceptionThrown());
   }

   public void testParallelException() throws InterruptedException {
      final ExecutorAllCompletionService service = createService(2);
      for (int i = 0; i < 150; ++i) {
         service.submit(new WaitRunnable(10), null);
      }
      service.submit(new ExceptionRunnable("foobar"), null);
      for (int i = 0; i < 150; ++i) {
         service.submit(new WaitRunnable(10), null);
      }
      List<Thread> threads = new ArrayList<>(10);
      for (int i = 0; i < 10; ++i) {
         Thread t = new Thread(() -> {
            service.waitUntilAllCompleted();
            assertTrue(service.isAllCompleted());
            assertTrue(service.isExceptionThrown());
         });
         threads.add(t);
         t.start();
      }
      for (Thread t : threads) {
         t.join();
      }
      assertTrue(service.isAllCompleted());
      assertTrue(service.isExceptionThrown());
   }

   private Throwable findCause(ExecutionException e) {
      Throwable t = e;
      while (t.getCause() != null) t = t.getCause();
      return t;
   }

   private ExecutorAllCompletionService createService(int maxThreads) {
      if (lastExecutorService != null) {
         lastExecutorService.shutdownNow();
      }

      lastExecutorService = Executors.newFixedThreadPool(maxThreads, getTestThreadFactory("Worker"));
      return new ExecutorAllCompletionService(lastExecutorService);
   }

   private class WaitRunnable implements Runnable {
      private long period;

      private WaitRunnable(long period) {
         this.period = period;
      }

      @Override
      public void run() {
         TestingUtil.sleepThread(period);
      }
   }

   private class ExceptionRunnable implements Runnable {
      private final String message;

      public ExceptionRunnable(String message) {
         this.message = message;
      }

      @Override
      public void run() {
         throw new RuntimeException(message);
      }
   }
}
