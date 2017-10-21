package org.infinispan.client.hotrod.impl.iteration;


import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;


/**
 * Remote iterator test with server compatibility mode enabled
 *
 * @author vjuranek
 * @since 8.2
 */
@Test(groups = "functional", testName = "client.hotrod.iteration.SingleServerCompatibilityRemoteIteratorTest")
public class SingleServerCompatibilityRemoteIteratorTest extends SingleServerRemoteIteratorTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = HotRodTestingUtil.hotRodCacheConfiguration();
      cb.compatibility().enable();
      return TestCacheManagerFactory.createServerModeCacheManager(cb);
   }

}
