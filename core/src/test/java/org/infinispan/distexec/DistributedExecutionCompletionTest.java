package org.infinispan.distexec;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Tests org.infinispan.distexec.DistributedExecutorService
 *
 * @author Vladimir Blagojevic
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "distexec.DistributedExecutionCompletionTest")
public class DistributedExecutionCompletionTest extends BaseDistFunctionalTest<Object, String> {

   private static Log log = LogFactory.getLog(DistributedExecutionCompletionTest.class);

   public DistributedExecutionCompletionTest() {
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
   }

   public void testBasicInvocation() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      try {
         DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);
         decs.submit(new SimpleCallable());
         NotifyingFuture<Integer> future = decs.take();
         Integer r = future.get();
         AssertJUnit.assertEquals(1, r.intValue());
      } finally {
         des.shutdownNow();
      }
   }

   public void testBasicDistributedCallableWitkKeys() throws Exception {
      c1.put("key1", "Manik");
      c1.put("key2", "Mircea");
      c1.put("key3", "Galder");
      c1.put("key4", "Sanne");

      DistributedExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Boolean> decs = new DistributedExecutionCompletionService<Boolean>(des);
      try {
         decs.submit(new SimpleDistributedCallable(true), new String[] { "key1", "key2" });
         Future<Boolean> future = decs.take();
         Boolean r = future.get();
         assert r;
      } finally {
         des.shutdownNow();
      }
   }

   public void testDistributedCallableEverywhereWithKeys() throws Exception {
      c1.put("key1", "Manik");
      c1.put("key2", "Mircea");
      c1.put("key3", "Galder");
      c1.put("key4", "Sanne");

      DefaultExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Boolean> decs = new DistributedExecutionCompletionService<Boolean>(des);
      try {
         decs.submitEverywhere(new SimpleDistributedCallable(true), new String[] { "key1", "key2" });
         Future<Boolean> f = null;
         int counter = 0;
         while ((f = decs.poll(1,TimeUnit.SECONDS)) != null) {
            assert f.get();
            counter++;
         }
         AssertJUnit.assertTrue("Counter greater than 0",  counter > 0);
      } finally {
         des.shutdownNow();
      }
   }

   public void testDistributedCallableEverywhere() throws Exception {

      DefaultExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Boolean> decs = new DistributedExecutionCompletionService<Boolean>(des);
      try {
         decs.submitEverywhere(new SimpleDistributedCallable(false));
         Future<Boolean> f = null;
         int counter = 0;
         while ((f = decs.poll(1,TimeUnit.SECONDS)) != null) {
            assert f.get();
            counter++;
         }
         AssertJUnit.assertTrue("Counter greater than 0",  counter > 0);
      } finally {
         des.shutdownNow();
      }
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testBasicInvocationWithNullExecutor() throws Exception {
      DistributedExecutorService des = null;
      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testBasicInvocationWithNullTask() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      try {
         DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);
         decs.submit(null);
      } finally {
         des.shutdownNow();
      }
   }

   public void testBasicInvocationWithBlockingQueue() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      try {
         BlockingQueue<NotifyingFuture<Integer>> queue = new ArrayBlockingQueue<NotifyingFuture<Integer>>(10);
         DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des, queue);
         decs.submit(new SimpleCallable());
         NotifyingFuture<Integer> future = decs.take();
         Integer r = future.get();
         AssertJUnit.assertEquals((Integer) 1, r);
      } finally {
         des.shutdownNow();
      }
   }

   public void testBasicInvocationWithRunnable() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      try {
         DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);
         Integer result = 5;
         decs.submit(new SimpleRunnable(), result);

         NotifyingFuture<Integer> future = decs.take();
         Integer r = future.get();
         AssertJUnit.assertEquals(result, r);
      } finally {
         des.shutdownNow();
      }
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testBasicInvocationWithNullRunnable() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      try {
         DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);
         Integer result = 5;
         decs.submit(null, result);
      } finally {
         des.shutdownNow();
      }
   }

   public void testBasicPollInvocationWithSleepingCallable() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);
      try {
         decs.submit(new SimpleCallable(true, 5000));
         NotifyingFuture<Integer> callable = decs.poll();
         AssertJUnit.assertNull(callable);
      } finally {
         des.shutdownNow();
      }
   }

   public void testBasicTakeInvocationWithSleepingCallable() throws Exception {
      long sleepTime = 2000;
      DistributedExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);
      try {
         long start = System.currentTimeMillis();
         decs.submit(new SimpleCallable(true, sleepTime));

         NotifyingFuture<Integer> future = decs.take();
         long end = System.currentTimeMillis();

         AssertJUnit.assertTrue("take() returned too soon", (end - start) >= sleepTime);
         AssertJUnit.assertTrue("take() returned, but future is not done yet", future.isDone());
         AssertJUnit.assertEquals((Integer) 1, future.get());
      } finally {
         des.shutdownNow();
      }
   }

   public void testBasicPollInvocation() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);
      try {
         decs.submit(new SimpleCallable());

         NotifyingFuture<Integer> callable = decs.poll(1000, TimeUnit.MILLISECONDS);

         AssertJUnit.assertEquals((Integer) 1, callable.get());
      } finally {
         des.shutdownNow();
      }
   }

   public void testBasicPollInvocationWithTimeout() throws Exception {
      DistributedExecutorService des = new DefaultExecutorService(c1);
      DistributedExecutionCompletionService<Integer> decs = new DistributedExecutionCompletionService<Integer>(des);
      try {
         decs.submit(new SimpleCallable(true,5000));
         NotifyingFuture<Integer> callable = decs.poll(10, TimeUnit.MILLISECONDS);

         AssertJUnit.assertNull(callable);
      } finally {
         des.shutdownNow();
      }
   }

   static class SimpleDistributedCallable implements DistributedCallable<String, String, Boolean>,
            Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = 623845442163221832L;
      private boolean invokedProperly = false;
      private final boolean hasKeys;

      public SimpleDistributedCallable(boolean hasKeys) {
         this.hasKeys = hasKeys;
      }

      @Override
      public Boolean call() throws Exception {
         return invokedProperly;
      }

      @Override
      public void setEnvironment(Cache<String, String> cache, Set<String> inputKeys) {
         boolean keysProperlySet = hasKeys ? inputKeys != null && !inputKeys.isEmpty()
                  : inputKeys != null && inputKeys.isEmpty();
         invokedProperly = cache != null && keysProperlySet;
      }

      public boolean validlyInvoked() {
         return invokedProperly;
      }
   }

   static class SimpleCallable implements Callable<Integer>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = -8589149500259272402L;
      private boolean shouldSleep = false;
      private long sleepTime;

      public SimpleCallable() {

      }

      public SimpleCallable(boolean shouldSleep, long sleepTime) {
         this.shouldSleep = shouldSleep;
         this.sleepTime = sleepTime;
      }

      @Override
      public Integer call() throws Exception {
         if(shouldSleep) {
            Thread.sleep(sleepTime);
         }

         return 1;
      }
   }

   static class SimpleRunnable implements Runnable, Serializable {

      @Override
      public void run() {
         log.trace("This is a runnable!");
      }
   }
}
