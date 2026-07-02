package org.infinispan.stats;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.stat.HeavyKeeper;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.stats.impl.DefaultHotKeyTracker;
import org.infinispan.stats.impl.DisabledHotKeyTracker;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "stats.HotKeyTrackingTest")
public class HotKeyTrackingTest extends MultipleCacheManagersTest {

   private static final int K = 10;

   @AfterMethod(alwaysRun = true)
   public void cleanUp() {
      for (String cacheName : manager(0).getCacheNames()) {
         Cache<?, ?> cache = manager(0).getCache(cacheName);
         cache.clear();
         cache.getAdvancedCache().getStats().reset();
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder tracked = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      tracked.statistics().enable().hotKeys().enabled(true).topK(K);

      ConfigurationBuilder untracked = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      untracked.statistics().enable();

      EmbeddedCacheManager ecm = addClusterEnabledCacheManager();
      ecm.defineConfiguration("tracked", tracked.build());
      ecm.defineConfiguration("untracked", untracked.build());
   }

   public void testDisabledByDefault() {
      Cache<String, String> cache = manager(0).getCache("untracked");
      HotKeyTracker tracker = TestingUtil.extractComponent(cache, HotKeyTracker.class);

      assertThat(tracker).isInstanceOf(DisabledHotKeyTracker.class);
      assertThat(tracker.totalReads()).isZero();
      assertThat(tracker.totalWrites()).isZero();
      assertThat(tracker.getTopReads(K)).isEmpty();
      assertThat(tracker.getTopWrites(K)).isEmpty();
   }

   public void testEnabledCreatesDefaultTracker() {
      Cache<String, String> cache = manager(0).getCache("tracked");
      HotKeyTracker tracker = TestingUtil.extractComponent(cache, HotKeyTracker.class);

      assertThat(tracker).isInstanceOf(DefaultHotKeyTracker.class);
   }

   public void testTopReadsTracked() {
      Cache<String, String> cache = manager(0).getCache("tracked");
      HotKeyTracker tracker = TestingUtil.extractComponent(cache, HotKeyTracker.class);

      cache.put("hot", "value");
      cache.put("warm", "value");
      cache.put("cold", "value");

      for (int i = 0; i < 500; i++) cache.get("hot");
      for (int i = 0; i < 100; i++) cache.get("warm");
      for (int i = 0; i < 10; i++) cache.get("cold");

      List<HeavyKeeper.KeyFrequency<Object>> topReads = tracker.getTopReads(K);
      List<Object> topKeys = topReads.stream().map(HeavyKeeper.KeyFrequency::key).toList();

      assertThat(topKeys).contains("hot", "warm", "cold");
      assertThat(topKeys.indexOf("hot")).isLessThan(topKeys.indexOf("warm"));
      assertThat(topKeys.indexOf("warm")).isLessThan(topKeys.indexOf("cold"));
      assertThat(tracker.totalReads()).isEqualTo(610);
   }

   public void testTopWritesTracked() {
      Cache<String, String> cache = manager(0).getCache("tracked");
      HotKeyTracker tracker = TestingUtil.extractComponent(cache, HotKeyTracker.class);

      for (int i = 0; i < 500; i++) cache.put("hot-write", "v" + i);
      for (int i = 0; i < 100; i++) cache.put("warm-write", "v" + i);
      for (int i = 0; i < 10; i++) cache.put("cold-write", "v" + i);

      List<HeavyKeeper.KeyFrequency<Object>> topWrites = tracker.getTopWrites(K);
      List<Object> topKeys = topWrites.stream().map(HeavyKeeper.KeyFrequency::key).toList();

      assertThat(topKeys).contains("hot-write", "warm-write", "cold-write");
      assertThat(topKeys.indexOf("hot-write")).isLessThan(topKeys.indexOf("warm-write"));
      assertThat(topKeys.indexOf("warm-write")).isLessThan(topKeys.indexOf("cold-write"));
      assertThat(tracker.totalWrites()).isEqualTo(610);
   }

   public void testWriteOnlyManyKeysTracked() throws Exception {
      Cache<String, String> cache = manager(0).getCache("tracked");
      HotKeyTracker tracker = TestingUtil.extractComponent(cache, HotKeyTracker.class);
      FunctionalMap.WriteOnlyMap<String, String> writeOnly = FunctionalMapImpl.create(cache.getAdvancedCache()).toWriteOnlyMap();

      for (int i = 0; i < 20; i++) {
         writeOnly.evalMany(Set.of("many-a", "many-b"), MarshallableFunctions.removeConsumer()).get(10, TimeUnit.SECONDS);
      }

      List<Object> writeKeys = tracker.getTopWrites(K).stream().map(HeavyKeeper.KeyFrequency::key).toList();
      assertThat(writeKeys).contains("many-a", "many-b");
      assertThat(tracker.totalWrites()).isEqualTo(40);
   }

   public void testWriteOnlyManyEntriesTracked() throws Exception {
      Cache<String, String> cache = manager(0).getCache("tracked");
      HotKeyTracker tracker = TestingUtil.extractComponent(cache, HotKeyTracker.class);
      FunctionalMap.WriteOnlyMap<String, String> writeOnly = FunctionalMapImpl.create(cache.getAdvancedCache()).toWriteOnlyMap();

      for (int i = 0; i < 20; i++) {
         writeOnly.evalMany(Map.of("entries-a", "v" + i, "entries-b", "v" + i), MarshallableFunctions.setValueConsumer())
               .get(10, TimeUnit.SECONDS);
      }

      List<Object> writeKeys = tracker.getTopWrites(K).stream().map(HeavyKeeper.KeyFrequency::key).toList();
      assertThat(writeKeys).contains("entries-a", "entries-b");
      assertThat(tracker.totalWrites()).isEqualTo(40);
   }

   public void testReadsAndWritesIndependent() {
      Cache<String, String> cache = manager(0).getCache("tracked");
      HotKeyTracker tracker = TestingUtil.extractComponent(cache, HotKeyTracker.class);

      for (int i = 0; i < 200; i++) cache.put("write-only", "v" + i);
      cache.put("read-target", "value");
      for (int i = 0; i < 200; i++) cache.get("read-target");

      List<Object> readKeys = tracker.getTopReads(K).stream().map(HeavyKeeper.KeyFrequency::key).toList();
      List<Object> writeKeys = tracker.getTopWrites(K).stream().map(HeavyKeeper.KeyFrequency::key).toList();

      assertThat(readKeys).contains("read-target");
      assertThat(writeKeys).contains("write-only");
   }

   public void testResetClearsTracking() {
      Cache<String, String> cache = manager(0).getCache("tracked");
      HotKeyTracker tracker = TestingUtil.extractComponent(cache, HotKeyTracker.class);

      for (int i = 0; i < 200; i++) {
         cache.put("k" + i, "v");
         cache.get("k" + i);
      }

      assertThat(tracker.totalReads()).isGreaterThan(0);
      assertThat(tracker.totalWrites()).isGreaterThan(0);

      tracker.reset();

      assertThat(tracker.totalReads()).isZero();
      assertThat(tracker.totalWrites()).isZero();
      assertThat(tracker.getTopReads(K)).isEmpty();
      assertThat(tracker.getTopWrites(K)).isEmpty();
   }

   public void testResetStatisticsClearsTracking() {
      Cache<String, String> cache = manager(0).getCache("tracked");
      HotKeyTracker tracker = TestingUtil.extractComponent(cache, HotKeyTracker.class);

      for (int i = 0; i < 100; i++) cache.put("key", "v" + i);

      assertThat(tracker.totalWrites()).isGreaterThan(0);

      cache.getAdvancedCache().getStats().reset();

      assertThat(tracker.totalWrites()).isZero();
      assertThat(tracker.getTopWrites(K)).isEmpty();
   }

   public void testMultipleCachesIndependentTracking() {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      cfg.statistics().enable().hotKeys().enabled(true).topK(K);

      manager(0).defineConfiguration("tracked-a", cfg.build());
      manager(0).defineConfiguration("tracked-b", cfg.build());

      Cache<String, String> cacheA = manager(0).getCache("tracked-a");
      Cache<String, String> cacheB = manager(0).getCache("tracked-b");

      HotKeyTracker trackerA = TestingUtil.extractComponent(cacheA, HotKeyTracker.class);
      HotKeyTracker trackerB = TestingUtil.extractComponent(cacheB, HotKeyTracker.class);

      for (int i = 0; i < 200; i++) cacheA.put("only-in-a", "v" + i);
      for (int i = 0; i < 200; i++) cacheB.put("only-in-b", "v" + i);

      List<Object> keysA = trackerA.getTopWrites(K).stream().map(HeavyKeeper.KeyFrequency::key).toList();
      List<Object> keysB = trackerB.getTopWrites(K).stream().map(HeavyKeeper.KeyFrequency::key).toList();

      assertThat(keysA).contains("only-in-a");
      assertThat(keysA).doesNotContain("only-in-b");

      assertThat(keysB).contains("only-in-b");
      assertThat(keysB).doesNotContain("only-in-a");
   }

   public void testDisabledTrackerIgnoresOperations() {
      Cache<String, String> cache = manager(0).getCache("untracked");
      HotKeyTracker tracker = TestingUtil.extractComponent(cache, HotKeyTracker.class);

      for (int i = 0; i < 100; i++) {
         cache.put("key-" + i, "value");
         cache.get("key-" + i);
      }

      assertThat(tracker.totalReads()).isZero();
      assertThat(tracker.totalWrites()).isZero();
      assertThat(tracker.getTopReads(K)).isEmpty();
      assertThat(tracker.getTopWrites(K)).isEmpty();
   }

   public void testTracksOnlyWhileStatisticsEnabled() {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      cfg.statistics().disable().hotKeys().enabled(true).topK(K);
      manager(0).defineConfiguration("statistics-off", cfg.build());

      Cache<String, String> cache = manager(0).getCache("statistics-off");
      HotKeyTracker tracker = TestingUtil.extractComponent(cache, HotKeyTracker.class);

      cache.put("before", "value");
      cache.get("before");
      assertThat(tracker.totalWrites()).isZero();
      assertThat(tracker.totalReads()).isZero();

      // Statistics can be enabled at runtime, as JMX does; tracking starts from that point.
      cache.getAdvancedCache().getStats().setStatisticsEnabled(true);
      cache.put("after", "value");
      cache.get("after");

      assertThat(tracker.totalWrites()).isEqualTo(1);
      assertThat(tracker.totalReads()).isEqualTo(1);
      assertThat(keys(tracker.getTopWrites(K))).containsExactly("after");
      assertThat(keys(tracker.getTopReads(K))).containsExactly("after");
   }

   public void testSkipStatisticsFlagNotTracked() {
      Cache<String, String> cache = manager(0).getCache("tracked");
      HotKeyTracker tracker = TestingUtil.extractComponent(cache, HotKeyTracker.class);

      for (int i = 0; i < 50; i++) {
         cache.getAdvancedCache().withFlags(Flag.SKIP_STATISTICS).put("skipped", "v" + i);
         cache.getAdvancedCache().withFlags(Flag.SKIP_STATISTICS).get("skipped");
      }

      assertThat(tracker.totalWrites()).isZero();
      assertThat(tracker.totalReads()).isZero();
      assertThat(tracker.getTopWrites(K)).isEmpty();
      assertThat(tracker.getTopReads(K)).isEmpty();
   }

   /**
    * Extracts the keys of a top-k result, keeping its order.
    *
    * @param top the top-k result
    * @return the keys, most frequent first
    */
   private static List<Object> keys(List<HeavyKeeper.KeyFrequency<Object>> top) {
      return top.stream().map(HeavyKeeper.KeyFrequency::key).toList();
   }

   public void testGetTopReadsRespectsLimit() {
      Cache<String, String> cache = manager(0).getCache("tracked");
      HotKeyTracker tracker = TestingUtil.extractComponent(cache, HotKeyTracker.class);

      for (int i = 0; i < K + 5; i++) {
         String key = "key-" + i;
         cache.put(key, "value");
         for (int j = 0; j < (K + 5 - i) * 10; j++) cache.get(key);
      }

      assertThat(tracker.getTopReads(3)).hasSize(3);
      assertThat(tracker.getTopReads(1)).hasSize(1);
   }
}
