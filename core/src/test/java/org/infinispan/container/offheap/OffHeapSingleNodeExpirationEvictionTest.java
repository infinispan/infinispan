package org.infinispan.container.offheap;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
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
}
