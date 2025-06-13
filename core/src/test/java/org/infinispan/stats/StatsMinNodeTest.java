package org.infinispan.stats;

import static org.testng.AssertJUnit.assertEquals;

import java.util.stream.IntStream;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests to make sure that minimum node output is correct
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "stats.StatsMinNodeTest")
public class StatsMinNodeTest extends MultipleCacheManagersTest {
   private static final int MAX_SIZE = 10;
   private static final int NUM_NODES = 5;
   private static final int NUM_OWNERS = 3;

   protected StorageType storageType;
   protected EvictionStrategy evictionStrategy;
   protected ControlledTimeService timeService;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(cacheMode, false);
      configure(cfg);

      for (int i = 0; i < NUM_NODES; ++i) {
         addClusterEnabledCacheManager(cfg);
      }

      waitForClusterToForm();

      timeService = new ControlledTimeService();

      for (int i = 0; i < NUM_NODES; ++i) {
         TestingUtil.replaceComponent(manager(i), TimeService.class, timeService, true);
      }
   }

   @AfterMethod
   public void cleanCache() {
      cache(0).clear();
      ClusterCacheStats stats = TestingUtil.extractComponent(cache(0), ClusterCacheStats.class);
      stats.reset();
      // This way each test will query stats
      timeService.advance(stats.getStaleStatsThreshold() + 1);
   }

   protected void configure(ConfigurationBuilder cfg) {
      cfg
         .statistics()
            .enable()
         .clustering()
            .hash()
               .numOwners(NUM_OWNERS)
         .memory()
            .storage(storageType)
            .maxCount(MAX_SIZE)
            .whenFull(evictionStrategy);
      if (evictionStrategy == EvictionStrategy.EXCEPTION) {
         cfg.transaction()
               .transactionMode(TransactionMode.TRANSACTIONAL);
      }
   }

   public StatsMinNodeTest withStorage(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   public StatsMinNodeTest withEvictionStrategy(EvictionStrategy evictionStrategy) {
      this.evictionStrategy = evictionStrategy;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new StatsMinNodeTest().withStorage(StorageType.HEAP).withEvictionStrategy(EvictionStrategy.REMOVE).cacheMode(CacheMode.DIST_SYNC),
            new StatsMinNodeTest().withStorage(StorageType.HEAP).withEvictionStrategy(EvictionStrategy.EXCEPTION).cacheMode(CacheMode.DIST_SYNC),
            new StatsMinNodeTest().withStorage(StorageType.HEAP).withEvictionStrategy(EvictionStrategy.REMOVE).cacheMode(CacheMode.REPL_SYNC),
            new StatsMinNodeTest().withStorage(StorageType.HEAP).withEvictionStrategy(EvictionStrategy.EXCEPTION).cacheMode(CacheMode.REPL_SYNC),
            new StatsMinNodeTest().withStorage(StorageType.OFF_HEAP).withEvictionStrategy(EvictionStrategy.REMOVE).cacheMode(CacheMode.DIST_SYNC),
            new StatsMinNodeTest().withStorage(StorageType.OFF_HEAP).withEvictionStrategy(EvictionStrategy.EXCEPTION).cacheMode(CacheMode.DIST_SYNC),
            new StatsMinNodeTest().withStorage(StorageType.OFF_HEAP).withEvictionStrategy(EvictionStrategy.REMOVE).cacheMode(CacheMode.REPL_SYNC),
            new StatsMinNodeTest().withStorage(StorageType.OFF_HEAP).withEvictionStrategy(EvictionStrategy.EXCEPTION).cacheMode(CacheMode.REPL_SYNC),
      };
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "StorageType", "EvictionStrategy");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), storageType, evictionStrategy);
   }

   @Override
   protected String parameters() {
      return "[" + cacheMode + ", " + storageType + "," + evictionStrategy + "]";
   }

   private int handleModeEstimate(int desired, CacheMode mode) {
      if (mode.isReplicated()) {
         // A REPL cache always requires just 1 node as all nodes have all data
         return 1;
      }
      return desired;
   }

   @DataProvider(name = "capacityTest")
   public static Object[][] capacityArguments() {
      int numOwnerMin = NUM_NODES - NUM_OWNERS + 1;

      // At this point data should matter
      int cutOff = MAX_SIZE * NUM_OWNERS / NUM_NODES;

      return IntStream.rangeClosed(0, MAX_SIZE).mapToObj(i -> {
         // 0 - 6 go through here
         if (i <= cutOff) {
            return new Object[] {i, numOwnerMin};
         } else {
            int totalData = i * NUM_OWNERS;
            // If there is a remainder of data left, another node has to pick up those as well
            boolean needExtra = totalData % MAX_SIZE != 0;

            // Min is required because of evictions throwing off calculation as we can only
            // store 16.66 repeating inserts without eviction firing off
            return new Object[] {i, Math.min(5, totalData / MAX_SIZE + (needExtra ? 1 : 0))};
         }
      }).toArray(Object[][]::new);
   }

   @Test(dataProvider = "capacityTest")
   public void testCapacity(int inserts, int nodeExpected) {
      caches().forEach(c -> {
         DataContainer container = c.getAdvancedCache().getDataContainer();
         // Just reuse same value
         Object value = c.getAdvancedCache().getValueDataConversion().toStorage("foo");
         Metadata metadata = new EmbeddedMetadata.Builder().build();
         // We just insert into data container directly - as we can guarantee same size then (this can't happen
         // in a real cache, but we can control the size easily)
         for (int i = 0; i < inserts; ++i) {
            Object key = c.getAdvancedCache().getKeyDataConversion().toStorage(i);
            container.put(key, value,  metadata);
         }
      });

      ClusterCacheStats stats = TestingUtil.extractComponent(cache(0), ClusterCacheStats.class);

      assertEquals(handleModeEstimate(nodeExpected, cacheMode), stats.getRequiredMinimumNumberOfNodes());
   }
}
