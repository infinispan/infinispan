package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.distribution.DistributionTestHelper.isFirstOwner;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "distexec.mapreduce.SimpleMapReduceTaskTimeoutTest")
public class SimpleMapReduceTaskTimeoutTest extends MultipleCacheManagersTest {

   private static final int REPLICATION_TIMEOUT = 5000;
   private static final int DEFAULT_TIMEOUT = 0;

   public SimpleMapReduceTaskTimeoutTest() {
      this.cleanup = CleanupPhase.AFTER_METHOD;
   }

   /**
    * Tests a map/reduce task with duration between replication timeout and task timeout (no exception should be
    * thrown)
    */
   public void testTimeout() {
      final int taskTimeout = REPLICATION_TIMEOUT * 4;
      final int sleepTime = REPLICATION_TIMEOUT * 2;
      final String sleepOnKey = init();

      MapReduceTask<String, String, String, Integer> task = createMapReduceTask(this.<String, String>cache(0));
      assertEquals("Wrong task timeout.", DEFAULT_TIMEOUT, task.timeout(MILLISECONDS));
      assertEquals("Wrong replication timeout.", REPLICATION_TIMEOUT,
                   cache(0).getCacheConfiguration().clustering().sync().replTimeout());
      task.timeout(taskTimeout, MILLISECONDS);
      assertEquals("Wrong new task timeout.", taskTimeout, task.timeout(MILLISECONDS));

      task.mappedWith(new SleepMapper(sleepTime, sleepOnKey))
            .reducedWith(new DummyReducer());

      long start = System.nanoTime();
      task.execute();
      long duration = System.nanoTime() - start;
      assertTrue(NANOSECONDS.toMillis(duration) >= sleepTime);
   }

   /**
    * Tests a map/reduce task with duration between task timeout and replication timeout (exception expected!)
    */
   public void testTimeout2() throws Exception {
      testTimeoutHelper(true);
   }

   /**
    * Tests async map/reduce task with duration between task timeout and replication timeout (exception expected!)
    */
   public void testTimeoutAsync() throws Exception {
      testTimeoutHelper(false);
   }

   /**
    * Tests async map/reduce task with duration between task timeout and replication timeout (exception expected!)
    */
   private void testTimeoutHelper(boolean sync) throws Exception {
      final int taskTimeout = REPLICATION_TIMEOUT / 4;
      final int sleepTime = REPLICATION_TIMEOUT / 2;
      final String sleepOnKey = init();

      MapReduceTask<String, String, String, Integer> task = createMapReduceTask(this.<String, String> cache(0));
      assertEquals("Wrong task timeout.", DEFAULT_TIMEOUT, task.timeout(MILLISECONDS));
      assertEquals("Wrong replication timeout.", REPLICATION_TIMEOUT, cache(0).getCacheConfiguration().clustering()
            .sync().replTimeout());
      task.timeout(taskTimeout, MILLISECONDS);
      assertEquals("Wrong new task timeout.", taskTimeout, task.timeout(MILLISECONDS));

      task.mappedWith(new SleepMapper(sleepTime, sleepOnKey)).reducedWith(new DummyReducer());

      long start = System.nanoTime();
      if (sync) {
         try {
            task.execute();
            fail("Should have gotten an exception for task.execute() call");
         } catch (CacheException expected) {
            assertTrue(hasTimeoutException(expected));
         }
      } else {
         try {
            Future<Map<String, Integer>> future = task.executeAsynchronously();
            future.get();
            fail("Should have gotten an exception for future.get() call");
         } catch (ExecutionException e) {
            assertTrue(hasTimeoutException(e));
         }
      }
      long duration = System.nanoTime() - start;
      assertTrue(NANOSECONDS.toMillis(duration) >= taskTimeout);
   }

   /**
    * Tests a map/reduce task with duration higher than replication timeout and waiting forever (no exception should be
    * thrown)
    */
   public void testNegativeTimeout() {
      final int taskTimeout = -1;
      final int sleepTime = REPLICATION_TIMEOUT * 2; //higher thant the replication timeout
      final String sleepOnKey = init();

      MapReduceTask<String, String, String, Integer> task = createMapReduceTask(this.<String, String>cache(0));
      assertEquals("Wrong task timeout.", DEFAULT_TIMEOUT, task.timeout(MILLISECONDS));
      assertEquals("Wrong replication timeout.", REPLICATION_TIMEOUT,
                   cache(0).getCacheConfiguration().clustering().sync().replTimeout());
      task.timeout(taskTimeout, MILLISECONDS);
      assertEquals("Wrong new task timeout.", taskTimeout, task.timeout(MILLISECONDS));

      task.mappedWith(new SleepMapper(sleepTime, sleepOnKey))
            .reducedWith(new DummyReducer());

      long start = System.nanoTime();
      task.execute();
      long duration = System.nanoTime() - start;
      assertTrue(NANOSECONDS.toMillis(duration) >= sleepTime);
   }

   /**
    * Tests a map/reduce task with duration higher than replication timeout and waiting forever (no exception should be
    * thrown)
    */
   public void testZeroTimeout() {
      final int taskTimeout = 0;
      final int sleepTime = REPLICATION_TIMEOUT * 2; //higher thant the replication timeout
      final String sleepOnKey = init();

      MapReduceTask<String, String, String, Integer> task = createMapReduceTask(this.<String, String>cache(0));
      assertEquals("Wrong task timeout.", DEFAULT_TIMEOUT, task.timeout(MILLISECONDS));
      assertEquals("Wrong replication timeout.", REPLICATION_TIMEOUT,
                   cache(0).getCacheConfiguration().clustering().sync().replTimeout());
      task.timeout(taskTimeout, MILLISECONDS);
      assertEquals("Wrong new task timeout.", taskTimeout, task.timeout(MILLISECONDS));

      task.mappedWith(new SleepMapper(sleepTime, sleepOnKey))
            .reducedWith(new DummyReducer());

      long start = System.nanoTime();
      task.execute();
      long duration = System.nanoTime() - start;
      assertTrue(NANOSECONDS.toMillis(duration) >= sleepTime);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.clustering().sync().replTimeout(REPLICATION_TIMEOUT);
      createClusteredCaches(4, builder);
   }

   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache<String, String> c) {
      return new MapReduceTask<String, String, String, Integer>(c);
   }

   private boolean hasTimeoutException(Exception exception) {
      Throwable iterator = exception;
      while (iterator != null) {
         if (iterator instanceof TimeoutException) {
            return true;
         }
         iterator = iterator.getCause();
      }
      return false;
   }

   /**
    * Initializes all the caches with some data.
    *
    * @return a key owned by cache(1).
    */
   private String init() {
      String sleepOnKey = null;
      for (int i = 0; i < 100; ++i) {
         String key = String.valueOf(i);
         cache(0).put(key, "v" + (i % 4));
         if (sleepOnKey == null && isFirstOwner(cache(1), key)) {
            sleepOnKey = key;
         }
      }
      return sleepOnKey;
   }

   public static class SleepMapper implements Mapper<String, String, String, Integer> {

      private final long sleepTime;
      private final String sleepOnKey;
      private boolean alreadySlept;

      public SleepMapper(long sleepTime, String sleepOnKey) {
         this.sleepTime = sleepTime;
         this.sleepOnKey = sleepOnKey;
         this.alreadySlept = false;
      }

      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
         if (!alreadySlept && key.equals(sleepOnKey)) {
            try {
               Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
               //no-op
               Thread.currentThread().interrupt();
            }
            alreadySlept = true;
         }
         collector.emit(value, 1);
      }
   }

   public static class DummyReducer implements Reducer<String, Integer> {

      @Override
      public Integer reduce(String reducedKey, Iterator<Integer> iter) {
         int sum = 0;
         while (iter.hasNext()) {
            sum += iter.next();
         }
         return sum;
      }
   }
}
