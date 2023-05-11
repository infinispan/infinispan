package org.infinispan.api;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.invoke.MethodHandles;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Tests if clear operation actually succeeds in removing all keys from all nodes of a distributed cluster.
 * See https://issues.jboss.org/browse/ISPN-2530.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "api.ClearTest")
public class ClearTest extends MultipleCacheManagersTest {

   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   protected AdvancedCache<Integer, String> c0;
   protected AdvancedCache<Integer, String> c1;
   protected AdvancedCache<Integer, String> c2;

   @Override
   public Object[] factory() {
      return new Object[] {
         new ClearTest().cacheMode(CacheMode.DIST_SYNC).transactional(false),
         new ClearTest().cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC),
         new ClearTest().cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, transactional, transactional);
      builder.clustering().hash().numSegments(3)
         .stateTransfer().fetchInMemoryState(true)
         .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());

      if (transactional) {
         builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .lockingMode(lockingMode);
      }
      createCluster(builder, 3);
      waitForClusterToForm();

      c0 = advancedCache(0);
      c1 = advancedCache(1);
      c2 = advancedCache(2);
   }

   public void testClear() throws Exception {
      final int numKeys = 5;
      log.infof("Putting %d keys into cache ..", numKeys);
      for (int i = 0; i < numKeys; i++) {
         String value = "val_" + i;
         c0.put(i, value);

         // force all values into L1 of the other nodes
         assertEquals(value, c0.get(i));
         assertEquals(value, c1.get(i));
         assertEquals(value, c2.get(i));
      }
      log.info("Finished putting keys");

      DataContainer dc0 = c0.getDataContainer();
      DataContainer dc1 = c1.getDataContainer();
      DataContainer dc2 = c2.getDataContainer();

      assertTrue(dc0.size() > 0);
      assertTrue(dc1.size() > 0);
      assertTrue(dc2.size() > 0);

      log.info("Clearing cache ..");
      c0.clear();
      log.info("Finished clearing cache");

      assertEquals(0, dc0.size());
      assertEquals(0, dc1.size());
      assertEquals(0, dc2.size());
   }
}
