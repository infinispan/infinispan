package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.query.test.Person;
import org.infinispan.search.mapper.mapping.SearchMapping;
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
               .enable()
               .storage(LOCAL_HEAP)
               .addIndexedEntity(Person.class);
         cc = TestCacheManagerFactory.createCacheManager(cfg);
         Cache<?, ?> cache = cc.getCache();
         SearchMapping searchMapping = TestingUtil.extractComponent(cache, SearchMapping.class);

         assertFalse(searchMapping.isClose());

         cc.stop();

         assertTrue(searchMapping.isClose());
      } finally {
         // proper cleanup for exceptional execution
         TestingUtil.killCacheManagers(cc);
      }
   }
}
