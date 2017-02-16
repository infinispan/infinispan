package org.infinispan.marshall;

import static org.testng.AssertJUnit.fail;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.MarshalledValueSingleNodeTest")
public class MarshalledValueSingleNodeTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
      c.invocationBatching().enable().storeAsBinary().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c);
      cache = cm.getCache();
      return cm;
   }

   public void testNonSerializable() {
      try {
         cache.put("Hello", new Object());
         fail("Should have failed");
      }
      catch (CacheException expected) {
         log.trace("");
      }

      try {
         cache.put(new Object(), "Hello");
         fail("Should have failed");
      }
      catch (CacheException expected) {

      }
   }

}
