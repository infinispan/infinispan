package org.infinispan.executors;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorServiceImpl;
import org.testng.annotations.Test;

/**
 * Simple executor test
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "executors.BlockingTaskAwareExecutorServiceTest")
public class BlockingTaskAwareExecutorServiceTest extends AbstractInfinispanTest {

   private static final AtomicInteger THREAD_ID = new AtomicInteger(0);

   public void testSimpleExecution() throws Exception {
      BlockingTaskAwareExecutorService executorService = createExecutorService();
      try {
         final DoSomething doSomething = new DoSomething();
         executorService.execute(doSomething);

         Thread.sleep(100);

         assert !doSomething.isReady();
         assert !doSomething.isExecuted();

         doSomething.markReady();
         executorService.checkForReadyTasks();

         assert doSomething.isReady();

         eventually(doSomething::isExecuted);
      } finally {
         executorService.shutdownNow();
      }
   }

   public void testMultipleExecutions() throws Exception {
      BlockingTaskAwareExecutorServiceImpl executorService = createExecutorService();
      try {
         List<DoSomething> tasks = new LinkedList<>();

         for (int i = 0; i < 30; ++i) {
            tasks.add(new DoSomething());
         }

         tasks.forEach(executorService::execute);

         for (DoSomething doSomething : tasks) {
            assert !doSomething.isReady();
            assert !doSomething.isExecuted();
         }

         tasks.forEach(BlockingTaskAwareExecutorServiceTest.DoSomething::markReady);
         executorService.checkForReadyTasks();

         for (final DoSomething doSomething : tasks) {
            eventually(doSomething::isExecuted);
         }

      } finally {
         executorService.shutdownNow();
      }
   }

   private BlockingTaskAwareExecutorServiceImpl createExecutorService() {
      final ExecutorService realOne = new ThreadPoolExecutor(1, 2, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                                                             new DummyThreadFactory());
      return new BlockingTaskAwareExecutorServiceImpl(realOne, TIME_SERVICE);
   }

   public static class DummyThreadFactory implements ThreadFactory {

      @Override
      public Thread newThread(Runnable runnable) {
         return new Thread(runnable, "Remote-" + getClass().getSimpleName() + "-" + THREAD_ID.incrementAndGet());
      }
   }

   public static class DoSomething implements BlockingRunnable {

      private volatile boolean ready = false;
      private volatile boolean executed = false;

      @Override
      public final synchronized boolean isReady() {
         return ready;
      }

      @Override
      public final synchronized void run() {
         executed = true;
      }

      public final synchronized void markReady() {
         ready = true;
      }

      public final synchronized boolean isExecuted() {
         return executed;
      }
   }
}
