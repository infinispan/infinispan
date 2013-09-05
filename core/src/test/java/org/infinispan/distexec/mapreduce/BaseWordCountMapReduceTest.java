package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
   
   /**
    * Test helper - has to be public because it used in CDI module
    * 
    */
   @SuppressWarnings({ "rawtypes", "unchecked" })
   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache c){
      return new MapReduceTask<String, String, String, Integer>(c);
   }
   
   
   /**
    * Test helper - has to be public because it used in CDI module
    * 
    */
   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[],
            Mapper<String, String, String, Integer> mapper, Reducer<String, Integer> reducer)
            throws Exception {
      return invokeMapReduce(keys, mapper, reducer, true);
   }
   
   /**
    * Test helper - has to be public because it used in CDI module
    * 
    */
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
   
   /**
    * Test helper - has to be public because it used in CDI module
    * 
    */
   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[]) throws Exception{
      return invokeMapReduce(keys, true);
   }
   
   /**
    * Test helper - has to be public because it used in CDI module
    * 
    */
   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[], boolean useCombiner) throws Exception{
      return invokeMapReduce(keys,new WordCountMapper(), new WordCountReducer(), useCombiner);
   }

   public void testInvokeMapReduceOnAllKeys() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(null);
      Map<String, Integer> mapReduce = task.execute();
      verifyResults(mapReduce);
   }
   
   public void testInvokeMapReduceOnEmptyKeys() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(new String[] {});
      Map<String, Integer> mapReduce = task.execute();
      verifyResults(mapReduce);
   }

   public void testInvokeMapReduceOnAllKeysWithCombiner() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(null, true);
      Map<String, Integer> mapReduce = task.execute();
      verifyResults(mapReduce);
   }
   
   public void testCombinerDoesNotChangeResult() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(null, true);
      Map<String, Integer> mapReduce = task.execute();
      
 
      MapReduceTask<String,String,String,Integer> task2 = invokeMapReduce(null, false);
      Map<String, Integer> mapReduce2 = task2.execute();

      AssertJUnit.assertEquals(mapReduce2.get("Infinispan"), mapReduce.get("Infinispan"));
      AssertJUnit.assertEquals(mapReduce2.get("RedHat"), mapReduce.get("RedHat"));
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
   
   public void testInvokeMapReduceOnAllKeysAsync() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(null);
      Future<Map<String, Integer>> future = task.executeAsynchronously();
      Map<String, Integer> mapReduce = future.get();
      verifyResults(mapReduce);
   }

   public void testInvokeMapReduceOnSubsetOfKeys() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(new String[]{"1", "2", "3"});
      Map<String, Integer> mapReduce = task.execute();
      assertWordCount(countWords(mapReduce), 13);
   }
   
   public void testInvokeMapReduceOnSubsetOfKeysAsync() throws Exception {
      MapReduceTask<String,String,String,Integer> task = invokeMapReduce(new String[]{"1", "2", "3"});
      Future<Map<String, Integer>> future = task.executeAsynchronously();
      Map<String, Integer> mapReduce = future.get();
      assertWordCount(countWords(mapReduce), 13); 
   }
   
   protected void verifyResults(Map <String,Integer> result, Map <String,Integer> verifyAgainst) {
      assertTrue("Results should have at least 1 answer", result.size() > 0);
      for (Entry<String, Integer> e : result.entrySet()) {
         String key = e.getKey();
         Integer count = verifyAgainst.get(key);
         assertTrue("key '" + e.getKey() + "' does not have count " + count + " but " + e.getValue(), count.equals(e.getValue()));
      }      
   }
   
   protected int nodeCount(){
      return getCacheManagers().size();
   }
      
   public void testInvokeMapReduceOnAllKeysWithCollator() throws Exception {
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
   
   public void testInvokeMapReduceOnSubsetOfKeysWithCollator() throws Exception {
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
   
   public void testInvokeMapReduceOnAllKeysWithCollatorAsync() throws Exception {
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

  public void testInvokeMapReduceOnSubsetOfKeysWithCollatorAsync() throws Exception {
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

   @Test(expectedExceptions = CacheException.class)
   public void testCombinerForDistributedReductionWithException() throws Exception {
      MapReduceTask<String, String, String, Integer> task = invokeMapReduce(null);
      task.combinedWith(new Reducer<String, Integer>() {
         @Override
         public Integer reduce(String reducedKey, Iterator<Integer> iter) {
            //simulating exception
            int a = 4 / 0;

            return null;
         }
      });

      task.execute();
  }
  
  protected void assertWordCount(int actualWordCount, int expectedWordCount){
     assertTrue(" word count of " + actualWordCount + " incorrect , expected " + expectedWordCount, actualWordCount == expectedWordCount);
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

   static class WordCountMapper implements Mapper<String, String, String,Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = -5943370243108735560L;

      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
//         System.out.println("[" + Thread.currentThread().getName() + "] - " + key + " - " + value);
         if(value == null) throw new IllegalArgumentException("Key " + key + " has value " + value);
         StringTokenizer tokens = new StringTokenizer(value);
         while (tokens.hasMoreElements()) {
            String s = (String) tokens.nextElement();
            collector.emit(s, 1);
         }         
      }
   }

   static class WordCountReducer implements Reducer<String, Integer> {
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
         assertEquals(0, count);
         count++;
      }
   }

   private static class IsolationReducer implements Reducer<String, Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = 6069777605143824777L;
      private int count = 0;

      @Override
      public Integer reduce(String key, Iterator<Integer> iter) {
         assertEquals(0, count);
         count++;
         return count;
      }
   }
}
