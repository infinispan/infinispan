package org.infinispan.scattered.statetransfer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TransportFlags;

public abstract class AbstractStateTransferTest extends MultipleCacheManagersTest {
   protected static final String CACHE_NAME = "scattered";
   protected static final TransportFlags TRANSPORT_FLAGS = new TransportFlags().withFD(true).withMerge(true);
   protected ConfigurationBuilder defaultConfig;
   protected Cache<Object, Object> c1;
   protected Cache<Object, Object> c2;
   protected Cache<Object, Object> c3;

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

   @Override
   protected void createCacheManagers() throws Throwable {
      defaultConfig = getDefaultClusteredCacheConfig(CacheMode.SCATTERED_SYNC, false);
      defaultConfig.clustering().hash().numSegments(16);
      defaultConfig.clustering().biasAcquisition(biasAcquisition);
      defaultConfig.clustering().stateTransfer().fetchInMemoryState(true).chunkSize(3);
      createClusteredCaches(3, defaultConfig, TRANSPORT_FLAGS, CACHE_NAME);

      c1 = cache(0, CACHE_NAME);
      c2 = cache(1, CACHE_NAME);
      c3 = cache(2, CACHE_NAME);
   }
}
