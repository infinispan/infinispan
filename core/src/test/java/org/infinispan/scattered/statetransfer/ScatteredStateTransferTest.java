package org.infinispan.scattered.statetransfer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.testng.Assert.*;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional")
@CleanupAfterMethod
public class ScatteredStateTransferTest extends MultipleCacheManagersTest {
   private static final String CACHE_NAME = "scattered";
   private static final TransportFlags TRANSPORT_FLAGS = new TransportFlags().withFD(true).withMerge(true);
   private ConfigurationBuilder defaultConfig;
   private Cache<Object, Object> c1, c2, c3;
   private DISCARD d1, d2, d3;

   @Override
   protected void createCacheManagers() throws Throwable {
      defaultConfig = getDefaultClusteredCacheConfig(CacheMode.SCATTERED_SYNC, false);
      defaultConfig.clustering().hash().numSegments(16);
      defaultConfig.clustering().stateTransfer().fetchInMemoryState(true).chunkSize(3);
      createClusteredCaches(3, defaultConfig, TRANSPORT_FLAGS, CACHE_NAME);

      c1 = cache(0, CACHE_NAME);
      c2 = cache(1, CACHE_NAME);
      c3 = cache(2, CACHE_NAME);
      d1 = TestingUtil.getDiscardForCache(c1);
      d1.setExcludeItself(true);
      d2 = TestingUtil.getDiscardForCache(c2);
      d2.setExcludeItself(true);
      d3 = TestingUtil.getDiscardForCache(c3);
      d3.setExcludeItself(true);
   }

   protected List<MagicKey> init() {
      List<MagicKey> keys = new ArrayList<>();
      int numCaches = caches(CACHE_NAME).size();
      ThreadLocalRandom random = ThreadLocalRandom.current();
      for (int i = 0; i < 100; ++i) {
         MagicKey key = new MagicKey("key" + i, cache(i % numCaches, CACHE_NAME));
         cache(random.nextInt(numCaches), CACHE_NAME).put(key, "value" + i);
         keys.add(key);
      }
      return keys;
   }

   public void testNodeCrash() {
      List<MagicKey> keys = init();

      assertFalse(c2.getCacheManager().isCoordinator());
      d2.setDiscardAll(true);
      TestingUtil.blockUntilViewsReceived(30000, false, c1, c3);
      TestingUtil.waitForRehashToComplete(c1, c3);

      checkValuesInDC(keys, c1, c3);
   }

   public void testCoordCrash() {
      List<MagicKey> keys = init();

      assertTrue(c1.getCacheManager().isCoordinator());
      d1.setDiscardAll(true);
      TestingUtil.blockUntilViewsReceived(30000, false, c2, c3);
      TestingUtil.waitForRehashToComplete(c2, c3);

      checkValuesInDC(keys, c2, c3);
   }

   public void testNodeJoin() throws Exception {
      List<MagicKey> keys = init();
      Cache c4 = addClusterEnabledCacheManager(defaultConfig, TRANSPORT_FLAGS).getCache(CACHE_NAME);
      TestingUtil.blockUntilViewsReceived(30000, false, c1, c2, c3, c4);
      TestingUtil.waitForRehashToComplete(c1, c2, c3, c4);

      checkValuesInCache(keys, c1, c2, c3, c4);
   }

   protected void checkValuesInDC(List<MagicKey> keys, Cache... caches) {
      assert caches != null && caches.length > 0;
      for (Cache c : caches) {
         for (int i = 0; i < keys.size(); ++i) {
            assertHasValueInDC(c, keys.get(i), "value" + i);
         }
      }
   }

   protected void checkValuesInCache(List<MagicKey> keys, Cache... caches) {
      assert caches != null && caches.length > 0;
      for (Cache c : caches) {
         for (int i = 0; i < keys.size(); ++i) {
            assertEquals(c.get(keys.get(i)), "value" + i);
         }
      }
   }

   private void assertHasValueInDC(Cache c, Object key, Object value) {
      InternalCacheEntry entry = c.getAdvancedCache().getDataContainer().peek(key);
      assertNotNull(entry, "Missing " + key);
      assertEquals(entry.getValue(), value, "Incorrect value for key " + key);
   }

}
