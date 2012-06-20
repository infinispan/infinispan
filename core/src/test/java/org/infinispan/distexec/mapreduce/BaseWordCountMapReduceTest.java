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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.withCacheManager;

/**
 * BaseTest for MapReduceTask
 * 
 * @author Vladimir Blagojevic
 */
@Test
public abstract class BaseWordCountMapReduceTest extends MultipleCacheManagersTest {

   public BaseWordCountMapReduceTest() {
      cleanup = CleanupPhase.AFTER_TEST;
   }

   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }
   
   protected String cacheName(){
      return "mapreducecache";
   }
   
   @SuppressWarnings({ "rawtypes", "unchecked" })
   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[],
            Mapper<String, String, String, Integer> mapper, Reducer<String, Integer> reducer, boolean useCombiner)
            throws Exception {
      Cache c1 = cache(0, cacheName());
      Cache c2 = cache(1, cacheName());

      c1.put("1", "Hello world here I am");
      c2.put("2", "Infinispan rules the world");
      c1.put("3", "JUDCon is in Boston");
      c2.put("4", "JBoss World is in Boston as well");
      c1.put("12","JBoss Application Server");
      c2.put("15", "Hello world");
      c1.put("14", "Infinispan community");
      c2.put("15", "Hello world");

      c1.put("111", "Infinispan open source");
      c2.put("112", "Boston is close to Toronto");
      c1.put("113", "Toronto is a capital of Ontario");
      c2.put("114", "JUDCon is cool");
      c1.put("211", "JBoss World is awesome");
      c2.put("212", "JBoss rules");
      c1.put("213", "JBoss division of RedHat ");
      c2.put("214", "RedHat community");

      MapReduceTask<String, String, String, Integer> task = new MapReduceTask<String, String, String, Integer>(c1);
      task.mappedWith(mapper).reducedWith(reducer);
      if(useCombiner)
         task.combinedWith(reducer);
      
      if(keys != null && keys.length>0){
         task.onKeys(keys);
      } 
      return task; 
   }
   
   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[]) throws Exception{
      return invokeMapReduce(keys, false);
   }
   
   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[], boolean useCombiner) throws Exception{
      return invokeMapReduce(keys,new WordCountMapper(), new WordCountReducer(), useCombiner);
   }
   
   @Test(expectedExceptions={IllegalStateException.class})
   public void testImproperCacheStateForMapReduceTask() throws Exception {

      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(builder)){
         @Override
         public void call() throws Exception {
            Cache<Object, Object> cache = cm.getCache();
            MapReduceTask<Object, Object, String, Integer> task = new MapReduceTask<Object, Object, String, Integer>(
                  cache);
         }
      });
   }

   public void testinvokeMapReduceOnAllKeys() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(null);
      Map<String, Integer> mapReduce = task.execute();
      Integer count = mapReduce.get("Infinispan");
      assert count == 3;
      count = mapReduce.get("RedHat");
      assert count == 2;
   }
   
   public void testinvokeMapReduceOnAllKeysWithCombiner() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(null, true);
      Map<String, Integer> mapReduce = task.execute();
      Integer count = mapReduce.get("Infinispan");
      assert count == 3;
      count = mapReduce.get("RedHat");
      assert count == 2;
   }
   
   public void testCombinerDoesNotChangeResult() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(null, true);
      Map<String, Integer> mapReduce = task.execute();
      
 
      MapReduceTask<String,String,String,Integer> task2 = invokeMapReduce(null, false);
      Map<String, Integer> mapReduce2 = task2.execute();
      assert mapReduce2.get("Infinispan") == mapReduce.get("Infinispan");
      assert mapReduce2.get("RedHat") == mapReduce.get("RedHat");      
   }
   
   /**
    * Tests isolation as mapper and reducer get invoked across the cluster
    * https://issues.jboss.org/browse/ISPN-1041
    * 
    * @throws Exception
    */
   public void testMapperReducerIsolation() throws Exception{
      invokeMapReduce(null, new IsolationMapper(), new IsolationReducer(), false);
   }
   
   public void testinvokeMapReduceOnAllKeysAsync() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(null);
      Future<Map<String, Integer>> future = task.executeAsynchronously();
      Map<String, Integer> mapReduce = future.get();
      Integer count = mapReduce.get("Infinispan");
      assert count == 3;
      count = mapReduce.get("RedHat");
      assert count == 2;
   }

   public void testinvokeMapReduceOnSubsetOfKeys() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(new String[] { "1", "2", "3" });
      Map<String, Integer> mapReduce = task.execute();
      Integer count = mapReduce.get("Infinispan");
      assert count == 1;
      count = mapReduce.get("Boston");
      assert count == 1;
   }
   
   public void testinvokeMapReduceOnSubsetOfKeysAsync() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(new String[] { "1", "2", "3" });
      Future<Map<String, Integer>> future = task.executeAsynchronously();
      Map<String, Integer> mapReduce = future.get();
      Integer count = mapReduce.get("Infinispan");
      assert count == 1;
      count = mapReduce.get("Boston");
      assert count == 1;
   }
   
   public void testinvokeMapReduceOnAllKeysWithCollator() throws Exception {
       MapReduceTask<String,String,String,Integer> task = invokeMapReduce(null);
       Integer totalWords = task.execute(new Collator<String, Integer, Integer>() {
         
         @Override
         public Integer collate(Map<String, Integer> reducedResults) {
            int sum = 0;
            for (Entry<String, Integer> e : reducedResults.entrySet()) {
               sum += e.getValue();
            }
            return sum;
         }
      });
      assert totalWords == 56; 
   }

   public void testinvokeMapReduceOnSubsetOfKeysWithCollator() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(new String[] { "1", "2", "3" });
      Integer totalWords = task.execute(new Collator<String, Integer, Integer>() {
         
         @Override
         public Integer collate(Map<String, Integer> reducedResults) {
            int sum = 0;
            for (Entry<String, Integer> e : reducedResults.entrySet()) {
               sum += e.getValue();
            }
            return sum;
         }
      });     
      assert totalWords == 13;    
   }
   
   public void testinvokeMapReduceOnAllKeysWithCollatorAsync() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(null);
      Future<Integer> future = task.executeAsynchronously(new Collator<String, Integer, Integer>() {
        
        @Override
        public Integer collate(Map<String, Integer> reducedResults) {
           int sum = 0;
           for (Entry<String, Integer> e : reducedResults.entrySet()) {
              sum += e.getValue();
           }
           return sum;
        }
     });
     Integer totalWords = future.get(); 
     assert totalWords == 56; 
  }

  public void testinvokeMapReduceOnSubsetOfKeysWithCollatorAsync() throws Exception {
     MapReduceTask<String,String,String,Integer> task = invokeMapReduce(new String[] { "1", "2", "3" });
     Future<Integer> future = task.executeAsynchronously(new Collator<String, Integer, Integer>() {
        
        @Override
        public Integer collate(Map<String, Integer> reducedResults) {
           int sum = 0;
           for (Entry<String, Integer> e : reducedResults.entrySet()) {
              sum += e.getValue();
           }
           return sum;
        }
     });
     Integer totalWords = future.get();
     assert totalWords == 13;    
  }

   private static class WordCountMapper implements Mapper<String, String, String,Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = -5943370243108735560L;

      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
         StringTokenizer tokens = new StringTokenizer(value);
         while (tokens.hasMoreElements()) {
            String s = (String) tokens.nextElement();
            collector.emit(s, 1);
         }         
      }
   }

   private static class WordCountReducer implements Reducer<String, Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = 1901016598354633256L;

      @Override
      public Integer reduce(String key, Iterator<Integer> iter) {
         int sum = 0;
         while (iter.hasNext()) {
            Integer i = iter.next();
            sum += i;
         }
         return sum;
      }
   }
   
   private static class IsolationMapper implements Mapper<String, String, String,Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = 1993535517358319862L;
      private int count = 0;

      
      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
         assert count == 0;                
         count++;
      }
   }

   private static class IsolationReducer implements Reducer<String, Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = 6069777605143824777L;
      private int count = 0;

      @Override
      public Integer reduce(String key, Iterator<Integer> iter) {
         assert count == 0;
         count++;
         return count;
      }
   }
}
