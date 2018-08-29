package org.infinispan.it.endpoints;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests embedded and Hot Rod interoperability in a distributed clustered environment using byte array keys.
 *
 * @author Martin Gencur
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "it.interop.ByteArrayKeyDistEmbeddedHotRodTest")
public class ByteArrayKeyDistEmbeddedHotRodTest extends ByteArrayKeyReplEmbeddedHotRodTest {

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
