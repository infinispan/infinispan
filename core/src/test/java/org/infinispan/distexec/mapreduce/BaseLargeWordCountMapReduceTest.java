package org.infinispan.distexec.mapreduce;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.infinispan.Cache;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * BaseLargeWordCountMapReduceTest tests parallel execution of Map/Reduce using word count over
 * Shakespeare's Macbeth :-)
 *
 * @author Vladimir Blagojevic
 * @since 7.0
 */
@Test(groups = "stress", testName = "distexec.mapreduce.BaseLargeWordCountMapReduceTest")
public abstract class BaseLargeWordCountMapReduceTest extends MultipleCacheManagersTest {

   protected HashMap<String, Integer> counts = new HashMap<String, Integer>();

   public BaseLargeWordCountMapReduceTest() {
      cleanup = CleanupPhase.AFTER_TEST;
   }

   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }

   protected String cacheName() {
      return "largemapreducecache";
   }

   @BeforeClass(alwaysRun = true)
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      specifyWordCounts();
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache c) {
      return new MapReduceTask<String, String, String, Integer>(c);
   }

   public void testInvokeMapReduceOnAllKeys() throws Exception {
      MapReduceTask<String, String, String, Integer> task = invokeMapReduce(null);
      Map<String, Integer> mapReduce = task.execute();
      verifyResults(mapReduce);
   }

   public void testInvokeMapReduceOnAllKeysWithResultCache() throws Exception {
      String cacheName = "resultCache";
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      defineConfigurationOnAllManagers(cacheName, builder);
      try {
         MapReduceTask<String, String, String, Integer> task = invokeMapReduce(null);
         Cache c1 = cache(0, cacheName);
         task.execute(c1);
         verifyResults(c1);
         c1.clear();
      } finally {
         removeCacheFromCluster(cacheName);
      }
   }

   public void testInvokeMapReduceOnSubsetOfKeys() throws Exception {
      MapReduceTask<String, String, String, Integer> task = invokeMapReduce(new String[] { "1", "2", "3" });
      Map<String, Integer> mapReduce = task.execute();
      assertPartialWordCount(countWords(mapReduce));
   }

   public void testInvokeMapReduceOnSubsetOfKeysWithResultCache() throws Exception {
      String cacheName = "resultCache2";
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      defineConfigurationOnAllManagers(cacheName, builder);
      try {
         MapReduceTask<String, String, String, Integer> task = invokeMapReduce(new String[] { "1", "2", "3" });
         task.execute(cacheName);
         Cache c1 = cache(0, cacheName);
         assertPartialWordCount(countWords(c1));
         c1.clear();
      } finally {
         removeCacheFromCluster(cacheName);
      }
   }

   protected MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[],
         Mapper<String, String, String, Integer> mapper, Reducer<String, Integer> reducer) throws Exception {
      return invokeMapReduce(keys, mapper, reducer, true);
   }

   protected MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[]) throws Exception {
      return invokeMapReduce(keys, true);
   }

   protected MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[], boolean useCombiner)
         throws Exception {
      return invokeMapReduce(keys, new WordCountMapper(), new WordCountReducer(), useCombiner);
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   protected MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[],
         Mapper<String, String, String, Integer> mapper, Reducer<String, Integer> reducer, boolean useCombiner)
         throws Exception {

      Cache c1 = cache(0, cacheName());
      Cache c2 = cache(1, cacheName());
      FileLookup fileLookup = new FileLookup();
      InputStream is = fileLookup.lookupFile("mapreduce/macbeth.txt", getClass().getClassLoader());

      BufferedReader br = new BufferedReader(new InputStreamReader(is));
      String line = null;
      int lineCount = 0;
      while ((line = br.readLine()) != null) {
         if (Math.random() > 0.5) {
            c1.put(String.valueOf(lineCount), line);
         } else {
            c2.put(String.valueOf(lineCount), line);
         }
         lineCount++;
      }
      MapReduceTask<String, String, String, Integer> task = createMapReduceTask(c1);
      task.mappedWith(mapper).reducedWith(reducer);
      if (useCombiner)
         task.combinedWith(reducer);

      if (keys != null && keys.length > 0) {
         task.onKeys(keys);
      }
      return task;
   }

   protected void specifyWordCounts() {
      counts.put("brave", 2);
      counts.put("more.", 2);
      counts.put("over-credulous", 1);
      counts.put("the", 607);
      counts.put("MACBETH", 241);
   }

   protected void verifyResults(Map<String, Integer> result) {
      verifyResults(result, counts);
   }

   protected void verifyResults(Map<String, Integer> result, Map<String, Integer> verifyAgainst) {
      assertTrue("Results should have at least 1 answer", result.size() > 0);
      for (Entry<String, Integer> e : result.entrySet()) {
         String key = e.getKey();
         Integer value = e.getValue();
         Integer count = verifyAgainst.get(key);
         if (count == null)
            continue;
         assertTrue("key '" + key + "' does not have count " + count + " but " + value, count.equals(value));
      }
   }

   protected int countWords(Map<String, Integer> result) {
      int sum = 0;
      for (Entry<String, Integer> e : result.entrySet()) {
         sum += e.getValue();
      }
      return sum;
   }

   protected void assertTotalWordCount(int actualWordCount) {
      int expectedWordCount = 18299;
      assertTrue(" word count of " + actualWordCount + " incorrect , expected " + expectedWordCount,
            actualWordCount == expectedWordCount);
   }

   protected void assertPartialWordCount(int actualWordCount) {
      int expectedWordCount = 2;
      assertTrue(" word count of " + actualWordCount + " incorrect , expected " + expectedWordCount,
            actualWordCount == expectedWordCount);
   }

   static class WordCountMapper implements Mapper<String, String, String, Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = -5943370243108735560L;

      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
         if (value == null)
            throw new IllegalArgumentException("Key " + key + " has value " + value);
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
}
