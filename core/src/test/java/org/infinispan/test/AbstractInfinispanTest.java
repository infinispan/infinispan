package org.infinispan.test;

import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterTest;

import javax.transaction.TransactionManager;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertTrue;

/**
 * AbstractInfinispanTest is a superclass of all Infinispan tests.
 *
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class AbstractInfinispanTest {

   protected final Log log = LogFactory.getLog(getClass());

   private Set<Thread> spawnedThreads = new HashSet<Thread>();
   private final AtomicInteger spawnedThreadsCounter = new AtomicInteger(0);
   public static final TimeService TIME_SERVICE = new DefaultTimeService();

   @AfterTest(alwaysRun = true)
   protected void killSpawnedThreads() {
      for (Thread t : spawnedThreads) {
         if (t.isAlive())
            t.interrupt();
      }
   }

   protected void eventually(Condition ec, long timeout) {
      eventually(ec, timeout, 10);
   }

   protected void eventually(Condition ec, long timeout, int loops) {
      if (loops <= 0) {
         throw new IllegalArgumentException("Number of loops must be positive");
      }
      long sleepDuration = timeout / loops;
      if (sleepDuration == 0) {
         sleepDuration = 1;
      }
      try {
         for (int i = 0; i < loops; i++) {

            if (ec.isSatisfied()) return;
            Thread.sleep(sleepDuration);
         }
         assertTrue(ec.isSatisfied());
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!", e);
      }
   }

   protected Thread fork(Runnable r, boolean waitForCompletion) {
      final String name = "ForkThread-" + spawnedThreadsCounter.incrementAndGet() + "," + getClass().getSimpleName();
      log.tracef("About to start thread '%s' as child of thread '%s'", name, Thread.currentThread().getName());
      final Thread t = new Thread(new RunnableWrapper(r), name);
      spawnedThreads.add(t);
      t.start();
      if (waitForCompletion) {
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return t.getState().equals(Thread.State.TERMINATED);
            }
         });
      }
      return t;
   }

   protected <T> Future<T> fork(Runnable r, T result) {
      final String name = "ForkThread-" + spawnedThreadsCounter.incrementAndGet() + "," + getClass().getSimpleName();
      log.tracef("About to start thread '%s' as child of thread '%s'", name, Thread.currentThread().getName());
      FutureTask<T> future = new FutureTask<T>(new LoggingCallable(Executors.callable(r, result)));
      final Thread t = new Thread(future, name);
      spawnedThreads.add(t);
      t.start();
      return future;
   }

   protected <T> Future<T> fork(Callable<T> c) {
      final String name = "ForkThread-" + spawnedThreadsCounter.incrementAndGet() + "," + getClass().getSimpleName();
      log.tracef("About to start thread '%s' as child of thread '%s'", name, Thread.currentThread().getName());
      FutureTask<T> future = new FutureTask<T>(new LoggingCallable<T>(c));
      final Thread t = new Thread(future, name);
      spawnedThreads.add(t);
      t.start();
      return future;
   }

   protected ThreadFactory getTestThreadFactory(final String prefix) {
      final String className = getClass().getSimpleName();

      return new ThreadFactory() {
         private final AtomicInteger counter = new AtomicInteger(0);

         @Override
         public Thread newThread(Runnable r) {
            String threadName = prefix + "-" + counter.incrementAndGet() + "," + className;
            return new Thread(r, threadName);
         }
      };
   }

   protected void runConcurrently(Callable<Object>... tasks) throws Exception {
      Future<Object>[] movers = new Future[tasks.length];
      final CountDownLatch latch = new CountDownLatch(1);
      for (int i = 0; i < tasks.length; i++) {
         final Callable<Object> task = tasks[i];
         movers[i] = fork(new Callable<Object>() {
            public Object call() throws Exception {
               latch.await();

               task.call();
               return null;
            }
         });
      }

      latch.countDown();
      // check for any errors
      Exception exception = null;
      for (Future<Object> t : movers) {
         try {
            t.get(10, TimeUnit.SECONDS);
         } catch (Exception e) {
            log.debug("Exception in concurrent task", e);
            exception = e;
         }
      }
      if (exception != null) {
         throw exception;
      }
   }

   public final class RunnableWrapper implements Runnable {

      final Runnable realOne;

      public RunnableWrapper(Runnable realOne) {
         this.realOne = realOne;
      }

      @Override
      public void run() {
         try {
            log.trace("Started fork thread..");
            realOne.run();
         } catch (Throwable e) {
            log.trace("Exiting fork thread due to exception", e);
         } finally {
            log.trace("Exiting fork thread.");
         }
      }
   }


   protected void eventually(Condition ec) {
      eventually(ec, 10000);
   }

   protected interface Condition {
      public boolean isSatisfied() throws Exception;
   }

   private class LoggingCallable<T> implements Callable<T> {
      private final Callable<T> c;

      public LoggingCallable(Callable<T> c) {
         this.c = c;
      }

      @Override
      public T call() throws Exception {
         try {
            return c.call();
         } catch (Exception e) {
            log.trace("Exception in forked task", e);
            throw e;
         }
      }
   }

   public void safeRollback(TransactionManager transactionManager) {
      try {
         transactionManager.rollback();
      } catch (Exception e) {
         //ignored
      }
   }
}
