package org.infinispan.statetransfer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.testng.annotations.Test;

/**
 * Tests concurrent startup of replicated and distributed caches
 *
 * @author Dan Berindei
 * @since 10.0
 */
@Test(testName = "statetransfer.StateTransferTimestampsTest", groups = "functional")
public class StateTransferTimestampsTest extends MultipleCacheManagersTest {
   public static final String CACHE_NAME = "cache";
   private ControlledTimeService timeService;

   @Override
   public Object[] factory() {
      return new Object[]{
         // No need to test DIST_SYNC, it's exactly the same as REPL_SYNC
         new StateTransferTimestampsTest().cacheMode(CacheMode.REPL_SYNC),
         new StateTransferTimestampsTest().cacheMode(CacheMode.SCATTERED_SYNC),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(new ConfigurationBuilder(), 2);

      timeService = new ControlledTimeService();
      ConfigurationBuilder replConfig = new ConfigurationBuilder();
      replConfig.clustering().cacheMode(cacheMode).hash().numSegments(4);
      for (EmbeddedCacheManager manager : managers()) {
         TestingUtil.replaceComponent(manager, TimeService.class, timeService, true);
         manager.defineConfiguration(CACHE_NAME, replConfig.build());
      }
   }

   public void testStateTransfer() {
      // Insert a key on node 0
      AdvancedCache<Object, Object> cache0 = advancedCache(0, CACHE_NAME);
      cache0.put("lifespan", "value", 2, SECONDS);
      cache0.put("maxidle", "value", 10, SECONDS, 2, SECONDS);
      long created = timeService.wallClockTime();

      // Advance the time service and start node 1 triggering state transfer
      timeService.advance(SECONDS.toMillis(1));
      AdvancedCache<Object, Object> cache1 = advancedCache(1, CACHE_NAME);

      // Check the timestamps on node 1
      long accessed = timeService.wallClockTime();
      CacheEntry<Object, Object> lifespanEntry = cache1.getCacheEntry("lifespan");
      assertEquals(created, lifespanEntry.getCreated());
      assertEquals(-1, lifespanEntry.getLastUsed());
      CacheEntry<Object, Object> maxidleEntry = cache1.getCacheEntry("maxidle");
      assertEquals(created, maxidleEntry.getCreated());
      assertEquals(accessed, maxidleEntry.getLastUsed());

      // Advance the time service to expire the lifespan entry
      timeService.advance(SECONDS.toMillis(2));
      assertNull(cache1.getCacheEntry("lifespan"));
      assertNotNull(cache1.getCacheEntry("maxidle"));

      // Advance the time service a final time to expire the maxidle entry
      timeService.advance(SECONDS.toMillis(3));
      assertNull(cache1.getCacheEntry("maxidle"));
   }
}
