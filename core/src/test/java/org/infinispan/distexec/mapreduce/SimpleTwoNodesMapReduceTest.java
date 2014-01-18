package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * SimpleTwoNodesMapReduceTest tests Map/Reduce functionality using two Infinispan nodes and local
 * reduce
 *
 * @author Vladimir Blagojevic
 * @since 5.0
 */
@Test(groups = "functional", testName = "distexec.mapreduce.SimpleTwoNodesMapReduceTest")
public class SimpleTwoNodesMapReduceTest extends BaseWordCountMapReduceTest {

   private static final Log log = LogFactory.getLog(SimpleTwoNodesMapReduceTest.class);
   protected static final Map<String, CyclicBarrier> barriers = CollectionFactory.makeConcurrentMap();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      createClusteredCaches(2, cacheName(), builder);
   }

   @Test(expectedExceptions={CancellationException.class})
   public void testInvokeMapperCancellation() throws Exception {
      final CyclicBarrier barrier = new CyclicBarrier(nodeCount() + 1);
      final String name = this.getClass().getSimpleName();
      barriers.put(name, barrier);

      MapReduceTask<String, String, String, Integer> task = invokeMapReduce(null,
               new LatchMapper(name), new WordCountReducer());
      final Future<Map<String, Integer>> future = task.executeAsynchronously();
      Future<Boolean> cancelled = fork(new Callable<Boolean>() {

         @Override
         public Boolean call() throws Exception {
            //make sure that all nodes receive the command and...
            barrier.await(10, TimeUnit.SECONDS);

            //...are ready to be canceled
            return future.cancel(true);
         }
      });
      boolean mapperCancelled = false;
      Throwable root = null;
      try {
         future.get();
      } catch (Exception e) {
         root = e;
         while(root.getCause() != null){
            root = root.getCause();
         }
         mapperCancelled = root.getClass().equals(InterruptedException.class);
      }
      assertTrue("Mapper not cancelled, root cause " + root, mapperCancelled);
      assertTrue(cancelled.get());
      assertTrue(future.isDone());

      //Cancelling again - should return false
      boolean canceled = future.cancel(true);
      assertFalse(canceled);
      //now call get() again and it should throw CancellationException
      future.get();
   }

   static class LatchMapper implements Mapper<String, String, String, Integer> {

      private static final long serialVersionUID = 2518908878377582179L;
      private final String name;

      LatchMapper(String name) {
         this.name = name;
      }

      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
         CyclicBarrier barrier = barriers.get(name);

         try {
            barrier.await(10, TimeUnit.SECONDS);
            TimeUnit.MILLISECONDS.sleep(5000);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         } catch (Exception e) {
            log.error("Error in the mapping phase", e);
         }
      }
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInvokeMapReduceNullMapper() throws Exception {
      invokeMapReduce(null, null, null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInvokeMapReduceNullReducer() throws Exception {
      invokeMapReduce(null, new ExceptionMapper(false), null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInvokeMapReduceNullCombiner() throws Exception {
      Cache cacheObj = cache(0, cacheName());
      MapReduceTask<String, String, String, Integer> task = new MapReduceTask<String, String, String, Integer>(cacheObj);
      MapReduceTask<String, String, String, Integer> task1 = new MapReduceTask<String, String, String, Integer>(cacheObj);

      task.mappedWith(new ExceptionMapper(false)).reducedWith(new ExceptionReducer(false));
      task.combinedWith(null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInvokeMapReduceWithNullMasterCache() {
      createMapReduceTask(null);
   }

   @Test(expectedExceptions = CacheException.class)
   public void testInvokeMapReduceWithException() throws Exception {
      MapReduceTask<String, String, String, Integer> task = invokeMapReduce(null, new ExceptionMapper(true), new ExceptionReducer(false));
      task.execute();
   }

   @Test(expectedExceptions = CacheException.class)
   public void testInvokeMapWithReduceExceptionPhaseInLocalExecution() throws Exception {
      Cache cache1 = cache(0, cacheName());
      Cache cache2 = cache(1, cacheName());

      cache1.put("key1", "value1");
      cache2.put("key2", "valu2");
      cache2.put("key3", "valu2");
      cache2.put("key4", "valu2");
      cache2.put("key5", "valu2");
      MapReduceTask<String, String, String, Integer> task = new MapReduceTask<String, String, String, Integer>(cache1, true, false);
      task.mappedWith(new WordCountMapper()).
            reducedWith(new ExceptionReducer(true));

      Map<String, Integer> val = task.execute();
   }

   @Test(expectedExceptions = CacheException.class)
   public void testInvokeMapWithReduceExceptionPhaseInRemoteExecution() throws Exception {
      Cache cache1 = cache(0, cacheName());
      Cache cache2 = cache(1, cacheName());

      cache1.put("key1", "value1");
      cache2.put("key2", "valu2");
      cache2.put("key3", "valu2");
      cache2.put("key4", "valu2");
      cache2.put("key5", "valu2");
      MapReduceTask<String, String, String, Integer> task = new MapReduceTask<String, String, String, Integer>(cache1, true, false);
      task.mappedWith(new WordCountMapper()).
            reducedWith(new FailAfterSecondCallReducer());

      Map<String, Integer> val = task.execute();
   }

   public void testMapReduceTasksComparison() throws Exception {
      MapReduceTask<String, String, String, Integer> task = invokeMapReduce(null, new ExceptionMapper(false), new ExceptionReducer(false));
      MapReduceTask<String, String, String, Integer> task1 = invokeMapReduce(null, new ExceptionMapper(false), new ExceptionReducer(false));
      MapReduceTask<String, String, String, Integer> task2 = invokeMapReduce(null, new ExceptionMapper(false), new ExceptionReducer(false));
      MapReduceTask<String, String, String, Integer> task3 = null;
      MapReduceTask<String, String, String, Integer> task4 = task1;

      Object objectForCompare = new Object();

      Map taskMap = new HashMap();
      taskMap.put(task, 1);
      taskMap.put(task1, 2);


      assertNotNull(taskMap.get(task));
      assertNotNull(taskMap.get(task1));
      assertNull(taskMap.get(task2));

      assertFalse(task1.equals(task2));
      assertFalse(task1.equals(task3));
      assertFalse(task1.equals(objectForCompare));
      assertTrue(task1.equals(task4));

      Pattern p = Pattern.compile("MapReduceTask \\[mapper=\\S+, reducer=\\S+, combiner=\\S*, keys=\\S*, taskId=\\S+\\]");
      Matcher m = p.matcher(task1.toString());
      assertTrue(m.find());
   }

   @Test(expectedExceptions = ExecutionException.class)
   public void testInvokeAsynchronouslyWithException() throws Exception {
      MapReduceTask<String, String, String, Integer> task = invokeMapReduce(null, new ExceptionMapper(true), new ExceptionReducer(true));

      Future<Map<String, Integer>> futureMap = task.executeAsynchronously();
      Map<String, Integer> resultMap = futureMap.get();
   }

   @Test(expectedExceptions = ExecutionException.class)
   public void testInvokeAsynchronouslyWithCollatorAndException() throws Exception {
      MapReduceTask<String, String, String, Integer> task = invokeMapReduce(null, new ExceptionMapper(true), new ExceptionReducer(true));

      Future<Integer> futureMap = task.executeAsynchronously(new Collator<String, Integer, Integer>() {

         @Override
         public Integer collate(Map<String, Integer> reducedResults) {
            return 0;
         }
      });

      futureMap.get();
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testEnsureProperCacheState() throws Exception {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createClusteredCacheManager(builder)
      ) {
         @Override
         public void call() {
            try {
               Cache cache = cm.getCache();
               cache.stop();

               MapReduceTask<String, String, String, Integer> task = createMapReduceTask(cache);
            } catch(IllegalStateException ex) {
               assertNotNull(ex.getMessage());
               assertTrue(ex.getMessage().contains("Cache is in an invalid state:"));
               throw ex;
            }
         }
      });
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testEnsureProperCacheStateMode() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, true);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createClusteredCacheManager(builder)) {

         @SuppressWarnings("unused")
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            MapReduceTask<Object, Object, String, Integer> task = new MapReduceTask<Object, Object, String, Integer>(
                  cache);
         }
      });
   }

   @Test(expectedExceptions = CacheException.class)
   public void testCombinerWithException() throws Exception {
      MapReduceTask<String, String, String, Integer> task = invokeMapReduce(null);
      task.combinedWith(new ExceptionReducer(true));

      task.execute();
   }

   public void testIntermediateCompositeKeys() {
      MapReduceManagerImpl.IntermediateCompositeKey key = new MapReduceManagerImpl.IntermediateCompositeKey("task1", 1);
      MapReduceManagerImpl.IntermediateCompositeKey key1 = new MapReduceManagerImpl.IntermediateCompositeKey("task2", 2);
      MapReduceManagerImpl.IntermediateCompositeKey key2 = new MapReduceManagerImpl.IntermediateCompositeKey("task1", 1);

      //Composite keys with null taskID
      MapReduceManagerImpl.IntermediateCompositeKey key3 = new MapReduceManagerImpl.IntermediateCompositeKey(null, 1);
      MapReduceManagerImpl.IntermediateCompositeKey key4 = new MapReduceManagerImpl.IntermediateCompositeKey(null, 2);

      //Composite keys with null keys
      MapReduceManagerImpl.IntermediateCompositeKey key5 = new MapReduceManagerImpl.IntermediateCompositeKey("task3", null);
      MapReduceManagerImpl.IntermediateCompositeKey key6 = new MapReduceManagerImpl.IntermediateCompositeKey("task4", null);

      //Composite keys with null keys & taskIDs
      MapReduceManagerImpl.IntermediateCompositeKey key7 = new MapReduceManagerImpl.IntermediateCompositeKey(null, null);
      MapReduceManagerImpl.IntermediateCompositeKey key8 = new MapReduceManagerImpl.IntermediateCompositeKey(null, null);

      assertFalse(key.equals(key1));
      assertTrue(key.equals(key2));
      assertFalse(key.equals(null));
      assertFalse(key.equals(new String()));

      assertFalse(key3.equals(key4));
      assertFalse(key5.equals(key6));

      assertTrue(key7.equals(key8));

      assertFalse(key3.equals(key));
      assertFalse(key5.equals(key));

   }

   private static class ExceptionMapper implements Mapper<String, String, String,Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = -5943370243108735560L;
      private boolean throwException = false;

      public ExceptionMapper(boolean throwException) {
         this.throwException = throwException;
      }

      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
         if(value == null) throw new IllegalArgumentException("Key " + key + " has value " + value);

         if(throwException) {
            //simulating exception here
            int a = 4 / 0;
         }
      }
   }

   private static class ExceptionReducer implements Reducer<String, Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = 1901016598354633256L;
      private boolean throwException;

      public ExceptionReducer(boolean throwException) {
         this.throwException = throwException;
      }

      @Override
      public Integer reduce(String key, Iterator<Integer> iter) {
         if(throwException) {
            //simulating exception
            int a = 4 / 0;
         }

         return 0;
      }
   }

   private static class FailAfterSecondCallReducer implements Reducer<String, Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = 1901016598354633256L;
      private static int counter;

      @Override
      public Integer reduce(String key, Iterator<Integer> iter) {

         if(counter > 0) {
            //simulating exception
            int a = 4 / 0;
         }
         counter++;

         return 0;
      }
   }
}
