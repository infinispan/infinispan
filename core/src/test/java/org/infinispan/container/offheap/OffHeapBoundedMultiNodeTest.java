package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.fail;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "container.offheap.OffHeapBoundedMultiNodeTest")
public class OffHeapBoundedMultiNodeTest extends OffHeapMultiNodeTest {
   static final int EVICTION_SIZE = NUMBER_OF_KEYS + 1;

   private TransactionMode transactionMode;

   OffHeapBoundedMultiNodeTest transactionMode(TransactionMode mode) {
      this.transactionMode = mode;
      return this;
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), transactionMode);
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "transactionMode");
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      dcc.memory().storage(StorageType.OFF_HEAP).maxCount(EVICTION_SIZE);
      dcc.transaction().transactionMode(transactionMode);
      createCluster(dcc, 4);
      waitForClusterToForm();
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new OffHeapBoundedMultiNodeTest().transactionMode(TransactionMode.TRANSACTIONAL),
            new OffHeapBoundedMultiNodeTest().transactionMode(TransactionMode.NON_TRANSACTIONAL)
      };
   }

   public void testEviction() {
      for (int i = 0; i < EVICTION_SIZE * 4; ++i) {
         cache(0).put("key" + i, "value" + i);
      }

      for (Cache cache : caches()) {
         int size = cache.getAdvancedCache().getDataContainer().size();
         if (size > EVICTION_SIZE) {
            fail("Container size was: " + size + ", it is supposed to be less than or equal to " + EVICTION_SIZE);
         }
      }
   }
}
