package org.infinispan.it.endpoints;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests embedded, Hot Rod and REST in a distributed clustered environment.
 *
 * @author Martin Gencur
 * @since 6.0
 */
@Test(groups = "functional", testName = "it.interop.DistEmbeddedRestHotRodTest")
public class DistEmbeddedRestHotRodTest extends ReplEmbeddedRestHotRodTest {

   private final int numOwners = 1;

   @Override
   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory1 = new EndpointsCacheFactory<>(CacheMode.DIST_SYNC, numOwners, false).setup();
      cacheFactory2 = new EndpointsCacheFactory<>(CacheMode.DIST_SYNC, numOwners, false).setup();
   }

   @Override
   @AfterClass
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory1, cacheFactory2);
   }

}
