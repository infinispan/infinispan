package org.infinispan.it.endpoints;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests embedded and Hot Rod in a distributed clustered environment using byte array values.
 *
 * @author Martin Gencur
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "it.interop.ByteArrayValueDistEmbeddedHotRodTest")
public class ByteArrayValueDistEmbeddedHotRodTest extends ByteArrayValueReplEmbeddedHotRodTest {

   @Override
   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory1 = new EndpointsCacheFactory<>(CacheMode.DIST_SYNC, 1, false).setup();
      cacheFactory2 = new EndpointsCacheFactory<>(CacheMode.DIST_SYNC, 1, false).setup();
   }

   @Override
   @AfterClass
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory1, cacheFactory2);
   }

}
