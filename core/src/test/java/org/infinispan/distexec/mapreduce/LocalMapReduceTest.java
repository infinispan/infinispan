package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.test.TestingUtil.now;
import static org.testng.AssertJUnit.assertEquals;

/**
 * ReplicatedFourNodesMapReduceTest tests Map/Reduce functionality using four Infinispan nodes,
 * replicated reduce and individual per task intermediate key/value cache
 *
 * @author William Burns
 * @since 5.3
 */
@Test(groups = "functional", testName = "distexec.mapreduce.LocalMapReduceTest")
public class LocalMapReduceTest extends DistributedFourNodesMapReduceTest {

   public static final int EXPIRATION_TIMEOUT = 3000;
   public static final int EVICTION_CHECK_TIMEOUT = 2000;

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.LOCAL;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(false);
      cacheManagers.add(cacheManager);
   }

   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache c){
      return new MapReduceTask<String, String, String, Integer>(c, false, false);
   }

   public void testInvokeMapReduceOnSubsetOfKeysWithResultCache() throws Exception {
      String cacheName = "resultCache2";
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true);
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

   public void testInvokeMapReduceOnAllKeysWithResultCache() throws Exception {
      String cacheName = "resultCache";
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true);
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

   public void testFilterExpiredInvokingMap() throws Exception {
      String cacheName = "expiredResultCache";
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true);
      defineConfigurationOnAllManagers(cacheName, builder);
      Cache<String, String> c1 = cache(0, cacheName());
      try {
         final long lifespan = EXPIRATION_TIMEOUT;
         final long startTime = now();
         Map<String, String> dataIn = getDataIn();
         Collection<String> valuesIn = dataIn.values();
         c1.putAll(dataIn, lifespan, MILLISECONDS);
         expectCachedAndThenExpired(c1, valuesIn, startTime, lifespan);
         WordCountMapper mapper = new WordCountMapper();
         WordCountReducer reducer = new WordCountReducer();
         MapReduceTask<String, String, String, Integer> task = createMapReduceTask(c1);
         task.mappedWith(mapper).reducedWith(reducer);
         Map<String, Integer> result = task.execute();
         assertEquals(0, result.size());
      } finally {
         c1.clear();
         removeCacheFromCluster(cacheName);
      }
   }

   private void expectCachedAndThenExpired(Cache<String, String> cache, Collection<String> valuesIn, long startTime, long lifespan) throws Exception {
      List<String> values;
      while (true) {
         values = new ArrayList<>(cache.values());
         if (now() >= startTime + lifespan)
            break;
         expectUnorderedEquals(valuesIn, values);
         Thread.sleep(100);
      }

      while (now() < startTime + lifespan + EVICTION_CHECK_TIMEOUT) {
         values = new ArrayList<>(cache.values());
         if (values.size() == 0) return;
      }

      assertEquals(0, values.size());
   }

   public Map<String, String> getDataIn() {
      Map<String, String> dataIn = new HashMap<String, String>();
      dataIn.put("1", "Hello world here I am");
      dataIn.put("2", "Infinispan rules the world");
      dataIn.put("3", "JUDCon is in Boston");
      dataIn.put("4", "JBoss World is in Boston as well");
      dataIn.put("12","JBoss Application Server");
      dataIn.put("15", "Hello world");
      dataIn.put("14", "Infinispan community");
      dataIn.put("111", "Infinispan open source");
      dataIn.put("112", "Boston is close to Toronto");
      dataIn.put("113", "Toronto is a capital of Ontario");
      dataIn.put("114", "JUDCon is cool");
      dataIn.put("211", "JBoss World is awesome");
      dataIn.put("212", "JBoss rules");
      dataIn.put("213", "JBoss division of RedHat ");
      dataIn.put("214", "RedHat community");
      return dataIn;
   }

   private <T> boolean expectUnorderedEquals(Collection<T> h1, Collection<T> h2) {
      if (h1.size() != h2.size())
         return false;

      List<T> clone = new ArrayList<>(h2);
      for (T next : h1) {
         if (clone.contains(next))
            clone.remove(next);
         else
            return false;
      }
      return true;
   }

   @Override
   /**
    * Method is overridden so that there is 1 cache for what the test may think are different managers
    * since local only has 1 manager.
    */
   protected <A, B> Cache<A, B> cache(int index, String cacheName) {
      return super.cache(0, cacheName);
   }
}
