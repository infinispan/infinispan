package org.infinispan.query.blackbox;

import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Ensures the search factory is properly shut down.
 *
 * @author Manik Surtani
 * @version 4.2
 */
@Test(testName = "query.blackbox.SearchFactoryShutdownTest", groups = "functional")
public class SearchFactoryShutdownTest extends AbstractInfinispanTest {
   
   public void testCorrectShutdown() {
      CacheContainer cc = null;

      try {
         ConfigurationBuilder cfg = new ConfigurationBuilder();
         cfg
            .transaction()
               .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing()
               .index(Index.ALL)
               .addProperty("default.directory_provider", "ram")
               .addProperty("lucene_version", "LUCENE_CURRENT");
         cc = TestCacheManagerFactory.createCacheManager(cfg);
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
