package org.infinispan.config;

import org.infinispan.manager.CacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "config.ProgrammaticConfigurationTest")
public class ProgrammaticConfigurationTest extends AbstractInfinispanTest {

   public void testDefiningConfigurationOverridingConsistentHashClass() {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      Configuration c = new Configuration();
      c.setConsistentHashClass("org.infinispan.distribution.DefaultConsistentHash");
      Configuration oneCacheConfiguration = cm.defineConfiguration("oneCache", c);
      assert oneCacheConfiguration.equals(c);
   }

}
