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
 * Similiar as MultiNodeReplicatedTest, but uses transactional configuration for the Infinispan.
 *
 * @TODO enable the test when ISPN-2727 is fixed.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.MultiNodeReplicatedTxTest", enabled = false)
public class MultiNodeReplicatedTxTest extends MultiNodeReplicatedTest {

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

   //@TODO enable the test when ISPN-2727 is fixed.
   @Test(enabled = false)
   public void testIndexingWorkDistribution() throws Exception {

   }
}
