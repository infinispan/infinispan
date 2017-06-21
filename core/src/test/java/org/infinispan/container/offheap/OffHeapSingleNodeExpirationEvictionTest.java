package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.eviction.EvictionType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

/**
 * Tests to make sure that off heap generates entry data properly when expiration and eviction are present
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "container.offheap.OffHeapSingleNodeExpirationEvictionTest")
public class OffHeapSingleNodeExpirationEvictionTest extends OffHeapSingleNodeTest {
   private enum EXPIRE_TYPE {
      MORTAL,
      TRANSIENT,
      TRANSIENT_MORTAL
   }

   private EXPIRE_TYPE expirationType;
   private boolean eviction;

   private OffHeapSingleNodeExpirationEvictionTest expirationTest(EXPIRE_TYPE expirationType) {
      this.expirationType = expirationType;
      return this;
   }

   private OffHeapSingleNodeExpirationEvictionTest eviction(boolean enable) {
      eviction = enable;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new OffHeapSingleNodeExpirationEvictionTest().expirationTest(EXPIRE_TYPE.MORTAL).eviction(true),
            new OffHeapSingleNodeExpirationEvictionTest().expirationTest(EXPIRE_TYPE.MORTAL).eviction(false),
            new OffHeapSingleNodeExpirationEvictionTest().expirationTest(EXPIRE_TYPE.TRANSIENT).eviction(true),
            new OffHeapSingleNodeExpirationEvictionTest().expirationTest(EXPIRE_TYPE.TRANSIENT).eviction(false),
            new OffHeapSingleNodeExpirationEvictionTest().expirationTest(EXPIRE_TYPE.TRANSIENT_MORTAL).eviction(true),
            new OffHeapSingleNodeExpirationEvictionTest().expirationTest(EXPIRE_TYPE.TRANSIENT_MORTAL).eviction(false),
      };
   }

   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder defaultConfig) {
      // We don't want anything to expire or evict in the base test - just making sure the generation of metadata
      // for entries doesn't cause issues
      switch (expirationType) {
         case MORTAL:
            defaultConfig.expiration().lifespan(10, TimeUnit.MINUTES);
            break;
         case TRANSIENT:
            defaultConfig.expiration().maxIdle(10, TimeUnit.MINUTES);
            break;
         case TRANSIENT_MORTAL:
            defaultConfig.expiration().lifespan(10, TimeUnit.MINUTES).maxIdle(10, TimeUnit.MINUTES);
            break;
      }
      if (eviction) {
         defaultConfig.memory().evictionType(EvictionType.COUNT).size(1000);
      }
      return super.addClusterEnabledCacheManager(defaultConfig);
   }

   public void testEnsureCorrectStorage() {
      Cache<String, String> cache = cache(0);
      long beforeInsert = System.currentTimeMillis();
      cache.put("k", "v");
      long afterInsert = System.currentTimeMillis();

      Encoder encoder = cache.getAdvancedCache().getKeyEncoder();
      Wrapper wrapper = cache.getAdvancedCache().getKeyWrapper();

      CacheEntry<String, String> entry = cache.getAdvancedCache().getDataContainer().get(
            wrapper.wrap(encoder.toStorage("k")));
      assertNotNull(entry);
      long storedTime = TimeUnit.MINUTES.toMillis(10);
      switch (expirationType) {
         case MORTAL:
            assertEquals(storedTime, entry.getLifespan());
            assertEquals(-1, entry.getMaxIdle());
            assertBetweenTimes(beforeInsert, entry.getCreated(), afterInsert);
            assertEquals(-1, entry.getLastUsed());
            break;
         case TRANSIENT:
            assertEquals(-1, entry.getLifespan());
            assertEquals(storedTime, entry.getMaxIdle());
            assertEquals(-1, entry.getCreated());
            assertBetweenTimes(beforeInsert, entry.getLastUsed(), afterInsert);
            break;
         case TRANSIENT_MORTAL:
            assertEquals(storedTime, entry.getLifespan());
            assertEquals(storedTime, entry.getMaxIdle());
            assertBetweenTimes(beforeInsert, entry.getCreated(), afterInsert);
            assertBetweenTimes(beforeInsert, entry.getLastUsed(), afterInsert);
            break;
      }
   }

   void assertBetweenTimes(long beforeInsert, long middle, long afterInsert) {
      assertTrue("before insert: " + beforeInsert + ",created time: " + middle + ", after insert: " + afterInsert,
            beforeInsert <= middle && middle <= afterInsert);
   }
}
