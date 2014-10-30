package org.infinispan.tx.locking;

import javax.transaction.Transaction;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * @author Martin Gencur
 */
@Test(groups = "functional", testName = "tx.locking.SizeDistTxRepeatableReadTest")
public class SizeDistTxRepeatableReadTest extends MultipleCacheManagersTest {

   IsolationLevel isolation;

   Object k0;
   Object k1;

   public SizeDistTxRepeatableReadTest() {
      this.isolation = IsolationLevel.REPEATABLE_READ;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder conf = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      conf.clustering().hash().numOwners(1)
            .locking().isolationLevel(isolation)
            .transaction()
            .lockingMode(LockingMode.OPTIMISTIC)
            .transactionManagerLookup(new DummyTransactionManagerLookup());
      createCluster(conf, 2);
      waitForClusterToForm();
      k0 = getKeyForCache(0);
      k1 = getKeyForCache(1);
   }

   public void testSizeWithPreviousRead() throws Exception {
      preloadCacheAndCheckSize();

      tm(0).begin();
      //make sure we read k1 in this transaction
      assertEquals("v1", cache(0).get(k1));
      final Transaction tx1 = tm(0).suspend();

      //another tx working on the same keys
      tm(0).begin();
      //remove the key that was previously read in another tx
      cache(0).remove(k1);
      cache(0).put(k0, "v2");
      tm(0).commit();

      assertEquals(1, cache(0).size());
      assertEquals("v2", cache(0).get(k0));

      tm(0).resume(tx1);
      //we read k1 earlier so size() should take the key into account even though it was removed in another tx
      assertEquals(2, cache(0).size());
      //we did not read k0 previosly - getting the changed value
      assertEquals("v2", cache(0).get(k0));
      //we've read it before - getting original value
      assertEquals("v1", cache(0).get(k1));
      tm(0).commit();

      assertNull(cache(1).get(k1));
   }

   public void testSizeWithoutPreviousRead() throws Exception {
      preloadCacheAndCheckSize();

      tm(0).begin();
      //no reading of k1 here
      final Transaction tx1 = tm(0).suspend();

      //another tx working on the same keys
      tm(0).begin();
      //remove the key that was previously read in another tx
      cache(0).remove(k1);
      cache(0).put(k0, "v2");
      tm(0).commit();

      assertEquals(1, cache(0).size());
      assertEquals("v2", cache(0).get(k0));

      tm(0).resume(tx1);
      //we did not read k1 earlier so size() should reflect the other transaction's changes
      assertEquals(1, cache(0).size());
      //we did not read any keys previosly - getting original values
      assertEquals("v2", cache(0).get(k0));
      assertNull(cache(0).get(k1));
      tm(0).commit();

      assertNull(cache(1).get(k1));
   }

   protected void preloadCacheAndCheckSize() throws Exception {
      tm(0).begin();
      cache(0).put(k0, "v0");
      cache(0).put(k1, "v1");
      assertEquals(2, cache(0).size());
      tm(0).commit();
   }
}
