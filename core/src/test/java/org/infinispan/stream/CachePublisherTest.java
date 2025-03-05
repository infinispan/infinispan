package org.infinispan.stream;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CachePublisher;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Base test class for streams to verify proper behavior of all of the terminal operations for all of the various
 * stream classes
 */
@Test(groups = "functional")
public class CachePublisherTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = "testCache";
   protected boolean simpleCache;
   protected ConfigurationBuilder builderUsed;

   public CachePublisherTest() {
      // Default values, to be overridden in factory method
      cacheMode = CacheMode.LOCAL;
      transactional = false;
   }

   protected void enhanceConfiguration(ConfigurationBuilder builder) {
      // Do nothing to config by default, used by people who extend this
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new CachePublisherTest().simpleCache(),
            new CachePublisherTest(),
            new CachePublisherTest().transactional(),
            new CachePublisherTest().cacheMode(CacheMode.REPL_SYNC),
            new CachePublisherTest().cacheMode(CacheMode.REPL_SYNC).transactional(),
            new CachePublisherTest().cacheMode(CacheMode.DIST_SYNC),
            new CachePublisherTest().cacheMode(CacheMode.DIST_SYNC).transactional(),
      };
   }

   @Override
   public CachePublisherTest cacheMode(CacheMode cacheMode) {
      return (CachePublisherTest) super.cacheMode(cacheMode);
   }

   CachePublisherTest transactional() {
      transactional = true;
      return this;
   }

   CachePublisherTest simpleCache() {
      simpleCache = true;
      return this;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "simple");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), simpleCache);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      if (transactional) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      if (cacheMode.isClustered()) {
         builderUsed.clustering().stateTransfer().chunkSize(50);
         enhanceConfiguration(builderUsed);
         createClusteredCaches(3, CACHE_NAME, builderUsed);
      } else {
         enhanceConfiguration(builderUsed);
         EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builderUsed);
         cacheManagers.add(cm);
         builderUsed.simpleCache(simpleCache);
         cm.defineConfiguration(CACHE_NAME, builderUsed.build());
      }
   }

   protected <K, V> Cache<K, V> getCache(int index) {
      return cache(index, CACHE_NAME);
   }

   private CachePublisher<Integer, String> insertAndGetPublisher() {
      AdvancedCache<Integer, String> cache = this.<Integer, String>getCache(0).getAdvancedCache();
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      return cache.cachePublisher();
   }

   public void testKeyReduce() {
      CachePublisher<Integer, String> publisher = insertAndGetPublisher();

      CompletionStage<List<Integer>> stage = publisher.keyReduction(
            p -> Flowable.fromPublisher(p).filter(k -> k % 2 == 0)
                  .collect(Collectors.toList()).toCompletionStage()
            ,
            p -> Flowable.fromPublisher(p).reduce(
                  (l1, l2) -> {
                     l1.addAll(l2);
                     return l1;
                  }
            ).toCompletionStage());
      List<Integer> list = CompletionStages.join(stage);
      assertContainsInAnyOrder(List.of(0, 2, 4, 6, 8), list);
   }

   public void testEntryReduce() {
      CachePublisher<Integer, String> publisher = insertAndGetPublisher();

      CompletionStage<List<String>> stage = publisher.entryReduction(
            p -> Flowable.fromPublisher(p)
                  .filter(e -> e.getKey() % 2 == 0)
                  .map(CacheEntry::getValue)
                  .collect(Collectors.toList()).toCompletionStage()
            ,
            p -> Flowable.fromPublisher(p).reduce(
                  (l1, l2) -> {
                     l1.addAll(l2);
                     return l1;
                  }
            ).toCompletionStage());
      List<String> list = CompletionStages.join(stage);
      assertContainsInAnyOrder(List.of("0-value", "2-value", "4-value", "6-value", "8-value"), list);
   }

   public void testKeyPublisher() {
      CachePublisher<Integer, String> publisher = insertAndGetPublisher();

      SegmentPublisherSupplier<Integer> ps = publisher.keyPublisher(p -> Flowable.fromPublisher(p)
            .map(i -> i + 1));

      assertEquals(IntStream.range(0, 10).sum() + 10,
            (int) Flowable.fromPublisher(ps.publisherWithoutSegments())
                  .reduce(0, Integer::sum).blockingGet());
   }

   public void testEntryPublisher() {
      CachePublisher<Integer, String> publisher = insertAndGetPublisher();

      SegmentPublisherSupplier<String> ps = publisher.entryPublisher(p -> Flowable.fromPublisher(p)
            .map(e -> e.getValue().replace('a', '4')));

      assertContainsInAnyOrder(
            IntStream.range(0, 10).mapToObj(i -> i + "-v4lue").toList(),
            Flowable.fromPublisher(ps.publisherWithoutSegments()).toList().blockingGet()
      );
   }

   private <E> void assertContainsInAnyOrder(List<E> expected, List<E> actual) {
      assertEquals(expected.size(), actual.size());
      assertTrue("expected: " + expected + " actual: " + actual, expected.containsAll(actual));
   }
}
