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

import static org.infinispan.test.TestingUtil.withCacheManager;

import java.util.HashMap;
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

/**
 * BaseTest for MapReduceTask
 * 
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "distexec.BaseWordCountMapReduceTest")
public abstract class BaseWordCountMapReduceTest extends MultipleCacheManagersTest {
   
   protected static HashMap<String,Integer> counts = new HashMap<String, Integer>();
   static {      
      counts.put("of", 2);
      counts.put("open", 1);
      counts.put("is", 6);
      counts.put("source", 1);
      counts.put("JBoss", 5);
      counts.put("in", 2);
      counts.put("capital", 1);
      counts.put("world", 3);
      counts.put("Hello", 2);
      counts.put("Ontario", 1);
      counts.put("cool", 1);
      counts.put("JUDCon", 2);
      counts.put("Infinispan", 3);
      counts.put("a", 1);
      counts.put("awesome", 1);
      counts.put("Application", 1);
      counts.put("am", 1);
      counts.put("RedHat", 2);
      counts.put("Server", 1);
      counts.put("community", 2);
      counts.put("as", 1);
      counts.put("the", 1);
      counts.put("Toronto", 2);
      counts.put("close", 1);
      counts.put("to", 1);
      counts.put("division", 1);
      counts.put("here", 1);
      counts.put("Boston", 3);
      counts.put("well", 1);
      counts.put("World", 2);
      counts.put("I", 1);
      counts.put("rules", 2);
   }

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
   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache c){
      return new MapReduceTask<String, String, String, Integer>(c);
   }
   
   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[],
            Mapper<String, String, String, Integer> mapper, Reducer<String, Integer> reducer)
            throws Exception {
      return invokeMapReduce(keys, mapper, reducer, true);
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

      c1.put("111", "Infinispan open source");
      c2.put("112", "Boston is close to Toronto");
      c1.put("113", "Toronto is a capital of Ontario");
      c2.put("114", "JUDCon is cool");
      c1.put("211", "JBoss World is awesome");
      c2.put("212", "JBoss rules");
      c1.put("213", "JBoss division of RedHat ");
      c2.put("214", "RedHat community");
      
      MapReduceTask<String, String, String, Integer> task = createMapReduceTask(c1);
      task.mappedWith(mapper).reducedWith(reducer);
      if(useCombiner)
         task.combinedWith(reducer);
      
      if(keys != null && keys.length>0){
         task.onKeys(keys);
      } 
      return task; 
   }
   
   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[]) throws Exception{
      return invokeMapReduce(keys, true);
   }
   
   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[], boolean useCombiner) throws Exception{
      return invokeMapReduce(keys,new WordCountMapper(), new WordCountReducer(), useCombiner);
   }
   
   @Test(expectedExceptions={IllegalStateException.class})
   public void testImproperCacheStateForMapReduceTask() {

      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(builder)){
         
         @SuppressWarnings("unused")
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            MapReduceTask<Object, Object, String, Integer> task = new MapReduceTask<Object, Object, String, Integer>(
                  cache);
         }
      });
   }

   
   public void testinvokeMapReduceOnAllKeys() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(null);
      Map<String, Integer> mapReduce = task.execute();
      verifyResults(mapReduce);
   }
   
   public void testinvokeMapReduceOnAllKeysWithCombiner() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(null, true);
      Map<String, Integer> mapReduce = task.execute();
      verifyResults(mapReduce);
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
      verifyResults(mapReduce);
   }

   public void testinvokeMapReduceOnSubsetOfKeys() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(new String[] { "1", "2", "3" });
      Map<String, Integer> mapReduce = task.execute();
      assertWordCount(countWords(mapReduce), 13);
   }
   
   public void testinvokeMapReduceOnSubsetOfKeysAsync() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(new String[] { "1", "2", "3" });
      Future<Map<String, Integer>> future = task.executeAsynchronously();
      Map<String, Integer> mapReduce = future.get();
      assertWordCount(countWords(mapReduce), 13); 
   }
   
   protected void verifyResults(Map <String,Integer> result, Map <String,Integer> verifyAgainst){      
      for (Entry<String, Integer> e : result.entrySet()) {
         String key = e.getKey();
         Integer count = verifyAgainst.get(key);
         assert count.equals(e.getValue()): "key " + e.getKey() + " does not have count " + count + " but " + e.getValue();                    
      }      
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
       assertWordCount(totalWords, 56);  
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
      assertWordCount(totalWords, 13);    
   }
   
   public void testinvokeMapReduceOnAllKeysWithCollatorAsync() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(null);
      Future<Integer> future = task.executeAsynchronously(new Collator<String, Integer, Integer>() {
        
        @Override
        public Integer collate(final Map<String, Integer> reducedResults) {
           int sum = 0;
           for (Entry<String, Integer> e : reducedResults.entrySet()) {
              sum += e.getValue();
           }
           return sum;
        }
     });
     Integer totalWords = future.get(); 
     assertWordCount(totalWords, 56); 
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
     assertWordCount(totalWords, 13); 
  }
  
  protected void assertWordCount(int actualWordCount, int expectedWordCount){
     assert actualWordCount == expectedWordCount: " word count of " + actualWordCount + " incorrect , expected " + expectedWordCount; 
  }
  
   protected int countWords(Map<String, Integer> result) {
      int sum = 0;
      for (Entry<String, Integer> e : result.entrySet()) {
         sum += e.getValue();
      }
      return sum;
   }

   protected void verifyResults(Map<String, Integer> result) {
      verifyResults(result, counts);
   }

   private static class WordCountMapper implements Mapper<String, String, String,Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = -5943370243108735560L;

      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
         if(value == null) throw new IllegalArgumentException("Key " + key + " has value " + value);
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
            sum += iter.next();            
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
