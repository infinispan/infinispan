package org.infinispan.query.distributed;

import org.infinispan.Cache;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * The same as the MultiNodeDistributedTest, only the cache configuration is transactional.
 *
 * @author Anna Manukyan
 */
@Test(groups = "unstable", description = "ISPN-4165", testName = "query.distributed.MultiNodeDistributedTxTest")
public class MultiNodeDistributedTxTest extends MultiNodeDistributedTest {

   protected boolean transactionsEnabled() {
      return true;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws IOException {
      ConfigurationBuilderHolder holder = readFromXml();
      holder.getDefaultConfigurationBuilder().transaction().transactionMode(TransactionMode.TRANSACTIONAL);

      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(holder, false);
      cacheManagers.add(cacheManager);
      Cache<String, Person> cache = cacheManager.getCache();
      caches.add(cache);
      TestingUtil.waitForRehashToComplete(caches);

      return cacheManager;
   }
}
