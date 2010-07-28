package org.infinispan.marshall;

import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "marshall.MarshalledValueSingleNodeTest")
public class MarshalledValueSingleNodeTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      Configuration c = getDefaultStandaloneConfig(true);
      c.setInvocationBatchingEnabled(true);
      c.setUseLazyDeserialization(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c, true);
      cache = cm.getCache();
      return cm;
   }

   public void testNonSerializable() {
      try {
         cache.put("Hello", new Object());
         assert false : "Should have failed";
      }
      catch (CacheException expected) {
         System.out.println("");
      }

      try {
         cache.put(new Object(), "Hello");
         assert false : "Should have failed";
      }
      catch (CacheException expected) {

      }
   }

}
