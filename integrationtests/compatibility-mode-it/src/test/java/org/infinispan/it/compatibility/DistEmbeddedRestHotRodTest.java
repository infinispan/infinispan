package org.infinispan.it.compatibility;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests embedded, Hot Rod and REST compatibility in a distributed
 * clustered environment.
 *
 * @author Martin Gencur
 * @since 6.0
 */
@Test(groups = "functional", testName = "it.compatibility.DistEmbeddedRestHotRodTest")
public class DistEmbeddedRestHotRodTest extends ReplEmbeddedRestHotRodTest {

   private final int numOwners = 1;

   @Override
   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory1 = new CompatibilityCacheFactory<>(CacheMode.DIST_SYNC, numOwners, false).setup();
      cacheFactory2 = new CompatibilityCacheFactory<>(CacheMode.DIST_SYNC, numOwners, false).setup();
   }

   @Override
   @AfterClass
   protected void teardown() {
      CompatibilityCacheFactory.killCacheFactories(cacheFactory1, cacheFactory2);
   }

}
