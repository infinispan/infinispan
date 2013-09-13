package org.infinispan.it.compatibility;

import org.infinispan.commons.equivalence.ByteArrayEquivalence;
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
      cacheFactory1 = new CompatibilityCacheFactory<Object, Object>(CacheMode.DIST_SYNC, 1)
            .valueEquivalence(ByteArrayEquivalence.INSTANCE)
            .setup();
      cacheFactory2 = new CompatibilityCacheFactory<Object, Object>(CacheMode.DIST_SYNC, 1)
            .valueEquivalence(ByteArrayEquivalence.INSTANCE)
            .setup(cacheFactory1.getHotRodPort(), 100);
   }

   @Override
   @AfterClass
   protected void teardown() {
      CompatibilityCacheFactory.killCacheFactories(cacheFactory1, cacheFactory2);
   }

}
