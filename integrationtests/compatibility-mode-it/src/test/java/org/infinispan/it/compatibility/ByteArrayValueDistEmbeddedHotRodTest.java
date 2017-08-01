package org.infinispan.it.compatibility;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests embedded and Hot Rod compatibility in a distributed clustered environment using byte array values.
 *
 * @author Martin Gencur
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "it.compatibility.ByteArrayValueDistEmbeddedHotRodTest")
public class ByteArrayValueDistEmbeddedHotRodTest extends ByteArrayValueReplEmbeddedHotRodTest {

   @Override
   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory1 = new CompatibilityCacheFactory<>(CacheMode.DIST_SYNC, 1, false).setup();
      cacheFactory2 = new CompatibilityCacheFactory<>(CacheMode.DIST_SYNC, 1, false).setup();
   }

   @Override
   @AfterClass
   protected void teardown() {
      CompatibilityCacheFactory.killCacheFactories(cacheFactory1, cacheFactory2);
   }

}
