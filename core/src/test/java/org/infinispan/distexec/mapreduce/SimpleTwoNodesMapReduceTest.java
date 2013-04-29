/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.distexec.mapreduce;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.withCacheManager;

/**
 * SimpleTwoNodesMapReduceTest tests Map/Reduce functionality using two Infinispan nodes and local
 * reduce
 * 
 * @author Vladimir Blagojevic
 * @since 5.0
 */
@Test(groups = "functional", testName = "distexec.mapreduce.SimpleTwoNodesMapReduceTest")
public class SimpleTwoNodesMapReduceTest extends BaseWordCountMapReduceTest {
   
   
   private static AtomicInteger counter = null;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      createClusteredCaches(2, cacheName(), builder);
   }
   
   /**
    * This test is here intentionally as we can not share static counter variable among concurrently
    * executing subclasses of BaseWordCountMapReduceTest in our testsuite
    * 
    */
   @Test(expectedExceptions={CancellationException.class})
   public void testInvokeMapperCancellation() throws Exception {
      //Initializing the counter in test itself, so that when each time the test runs, the execution is correct
      // and is not based on previous values.
      counter = new AtomicInteger();

      MapReduceTask<String, String, String, Integer> task = invokeMapReduce(null,
               new LatchMapper(), new WordCountReducer());
      final Future<Map<String, Integer>> future = task.executeAsynchronously();
      Future<Boolean> cancelled = fork(new Callable<Boolean>() {

         @Override
         public Boolean call() throws Exception {
            //make sure that all nodes receive the command and...
            eventually(new Condition() {
               
               @Override
               public boolean isSatisfied() throws Exception {
                  return counter.get() >= nodeCount();
               }
            });
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
         mapperCancelled = root.getClass().equals(RuntimeException.class);         
      }
      assert mapperCancelled : "Mapper not cancelled, root cause " + root;
      assert cancelled.get();
      assert future.isDone();

      //Cancelling again - should return false
      boolean canceled = future.cancel(true);
      assert !canceled;
      //now call get() again and it should throw CancellationException
      future.get();
   }
   
   static class LatchMapper implements Mapper<String, String, String, Integer> {

      /** The serialVersionUID */
      private static final long serialVersionUID = 2518908878377582179L;      
      
      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
         boolean interrupted = false;
         CountDownLatch latch = new CountDownLatch(1);
         try {
            if (!interrupted) {
               counter.incrementAndGet();
               latch.await(5000, TimeUnit.MILLISECONDS);
            } else {
               interrupted = true;// already interrupted
            }
         } catch (InterruptedException e) {
            interrupted = true;
            Thread.currentThread().interrupt();
         }
         //as we can not throw InterruptedException 
         //throw a RuntimeException and check for it in the test...         
         if (interrupted) throw new RuntimeException();
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


      assert taskMap.get(task) != null;
      assert taskMap.get(task1) != null;
      assert taskMap.get(task2) == null;

      assert !task1.equals(task2);
      assert !task1.equals(task3);
      assert !task1.equals(objectForCompare);
      assert task1.equals(task4);

      Pattern p = Pattern.compile("MapReduceTask \\[mapper=\\S+, reducer=\\S+, combiner=\\S*, keys=\\S*, taskId=\\S+\\]");
      Matcher m = p.matcher(task1.toString());
      assert m.find();
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
      EmbeddedCacheManager cacheManager = addClusterEnabledCacheManager(builder);
      Cache cache = cacheManager.getCache();
      cache.stop();

      try {
         MapReduceTask<String, String, String, Integer> task = createMapReduceTask(cache);
      } catch(IllegalStateException ex) {
         assert ex.getMessage() != null && ex.getMessage().contains("Invalid cache state");
         throw ex;
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
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

      assert !key.equals(key1);
      assert key.equals(key2);
      assert !key.equals(null);
      assert !key.equals(new String());

      assert !key3.equals(key4);
      assert !key5.equals(key6);

      assert key7.equals(key8);

      assert !key3.equals(key);
      assert !key5.equals(key);

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
