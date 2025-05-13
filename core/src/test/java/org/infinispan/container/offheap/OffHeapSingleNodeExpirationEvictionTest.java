package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.encoding.DataConversion;
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
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "EXPIRE_TYPE", "eviction");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), expirationType, eviction);
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
            defaultConfig.expiration().maxIdle(5, TimeUnit.MINUTES);
            break;
         case TRANSIENT_MORTAL:
            defaultConfig.expiration().lifespan(10, TimeUnit.MINUTES).maxIdle(5, TimeUnit.MINUTES);
            break;
      }
      if (eviction) {
         defaultConfig.memory().evictionType(EvictionType.COUNT).size(1000);
      }
      return super.addClusterEnabledCacheManager(defaultConfig);
   }

   public void testEnsureCorrectStorage() {
      Cache<String, String> cache = cache(0);
      long beforeInsert = timeService.wallClockTime();
      cache.put("k", "v");
      timeService.advance(10);

      DataConversion dataConversion = cache.getAdvancedCache().getKeyDataConversion();

      Object convertedKey = dataConversion.toStorage("k");
      assertNotNull(cache.getAdvancedCache().getDataContainer().peek(convertedKey));

      CacheEntry<String, String> entry = cache.getAdvancedCache().getDataContainer().peek(convertedKey);

      assertNotNull(entry);
      long maxIdleTime = TimeUnit.MINUTES.toMillis(5);
      long storedTime = TimeUnit.MINUTES.toMillis(10);
      switch (expirationType) {
         case MORTAL:
            assertEquals(storedTime, entry.getLifespan());
            assertEquals(-1, entry.getMaxIdle());
            assertEquals(beforeInsert, entry.getCreated());
            assertEquals(-1, entry.getLastUsed());
            break;
         case TRANSIENT:
            assertEquals(-1, entry.getLifespan());
            assertEquals(maxIdleTime, entry.getMaxIdle());
            assertEquals(-1, entry.getCreated());
            assertEquals(beforeInsert, entry.getLastUsed());
            break;
         case TRANSIENT_MORTAL:
            assertEquals(storedTime, entry.getLifespan());
            assertEquals(maxIdleTime, entry.getMaxIdle());
            assertEquals(beforeInsert, entry.getCreated());
            assertEquals(beforeInsert, entry.getLastUsed());
            break;
      }
   }
}
