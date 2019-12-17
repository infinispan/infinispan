package org.infinispan.query.blackbox;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.CacheContainer;
import org.infinispan.query.test.Person;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Ensures the search factory is properly shut down.
 *
 * @author Manik Surtani
 * @since 4.2
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
               .addIndexedEntity(Person.class)
               .addProperty("default.directory_provider", "local-heap")
               .addProperty("lucene_version", "LUCENE_CURRENT");
         cc = TestCacheManagerFactory.createCacheManager(cfg);
         Cache<?, ?> cache = cc.getCache();
         SearchIntegrator sfi = TestingUtil.extractComponent(cache, SearchIntegrator.class);

         assertFalse(sfi.isStopped());

         cc.stop();

         assertTrue(sfi.isStopped());
      } finally {
         // proper cleanup for exceptional execution
         TestingUtil.killCacheManagers(cc);
      }
   }
}
