package org.infinispan.query.blackbox;

import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.config.Configuration.CacheMode.LOCAL;

/**
 * Ensures the search factory is properly shut down.
 *
 * @author Manik Surtani
 * @version 4.2
 */
@Test(testName = "query.blackbox.SearchFactoryShutdownTest", groups = "functional")
public class SearchFactoryShutdownTest extends AbstractInfinispanTest {
   
   public void testCorrectShutdown() throws NoSuchFieldException, IllegalAccessException {
      CacheContainer cc = null;

      try {
         Configuration c = SingleCacheManagerTest.getDefaultClusteredConfig(LOCAL, true);
         c.fluent().indexing().indexLocalOnly(false);
         cc = TestCacheManagerFactory.createCacheManager(c, true);
         Cache<?, ?> cache = cc.getCache();
         SearchFactoryIntegrator sfi = TestingUtil.extractComponent(cache, SearchFactoryIntegrator.class);

         assert ! sfi.isStopped();

         cc.stop();

         assert sfi.isStopped();
      } finally {
         // proper cleanup for exceptional execution
         TestingUtil.killCacheManagers(cc);
      }
   }

}
