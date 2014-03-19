package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.impl.TransactionTable;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.1
 */
@Test(groups = "functional", testName = "tx.RemoteTxNotCreatedOnGetTest")
public class RemoteTxNotCreatedOnGetTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.clustering().l1().disable().hash().numOwners(1);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   public void testRemoteTxCreation() throws Throwable {
      Object key = getKeyForCache(1);
      cache(1).put(key, "v");
      assertEquals("v", cache(0).get(key));
      assertEquals("v", cache(1).get(key));


      Thread.sleep(1000);

      TransactionTable tt1 = TestingUtil.getTransactionTable(cache(1));
      assertEquals(tt1.getRemoteTransactions().size(), 0);
      tm(0).begin();
      log.trace("Before going remotely");
      cache(0).get(key);
      assertEquals(tt1.getRemoteTransactions().size(), 0);
      tm(0).commit();
   }
}