package org.infinispan.api.mvcc;

import org.infinispan.Cache;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;

import javax.transaction.TransactionManager;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public abstract class PutForExternalReadTxBaseTest extends PutForExternalReadTest {

   protected boolean isOptimistic(Cache<?, ?> cache) {
      return cache.getAdvancedCache().getCacheConfiguration().transaction().lockingMode() == LockingMode.OPTIMISTIC;
   }

   public void testPutForExternalReadAfterClearRolledBack() throws Exception {
      final Cache<MagicKey, String> cache = cache(0, CACHE_NAME);

      final MagicKey myKey = new MagicKey(cache);
      cache.put(myKey, "v1");
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      try {
         cache.clear();
         cache.putForExternalRead(myKey, "v2");
         // The PFER is done outside this tx so the value is still removed in our context
         assertFalse(cache.containsKey(myKey));
      } finally {
         tm.rollback();
      }

      // PFER would fail since the value was already present and PFER operation is putIfAbsent
      assertEquals("v1", cache.get(myKey));
   }

   public void testPutForExternalReadAfterClearCommitted() throws Exception {
      final Cache<MagicKey, String> cache = cache(0, CACHE_NAME);

      final MagicKey myKey = new MagicKey(cache);
      cache.put(myKey, "v1");
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      try {
         cache.clear();
         cache.putForExternalRead(myKey, "v2");
         assertFalse(cache.containsKey(myKey));
      } finally {
         tm.commit();
      }

      // In either case committing the clear will remove the value
      assertFalse(cache.containsKey(myKey));
   }

   public void testNoValuePutForExternalReadAfterClearRolledBack() throws Exception {
      final Cache<MagicKey, String> cache = cache(0, CACHE_NAME);
      cache.put(new MagicKey(cache), "random-value");

      final MagicKey myKey = new MagicKey(cache);
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      try {
         cache.clear();
         cache.putForExternalRead(myKey, "v2");
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return cache.getAdvancedCache().getDataContainer().containsKey(myKey);
            }
         });
         // Even in pessimistic the clear will not hold the lock since clear only removes entries that exist
         assertTrue(cache.containsKey(myKey));
      } finally {
         tm.rollback();
      }

      assertEquals("v2", cache.get(myKey));
   }

   public void testPutForExternalReadAfterEmptyClearCommitted() throws Exception {
      final Cache<MagicKey, String> cache = cache(0, CACHE_NAME);
      cache.put(new MagicKey(cache), "random-value");

      final MagicKey myKey = new MagicKey(cache);
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      try {
         cache.clear();
         cache.putForExternalRead(myKey, "v2");
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return cache.getAdvancedCache().getDataContainer().containsKey(myKey);
            }
         });
         assertTrue(cache.containsKey(myKey));
      } finally {
         tm.commit();
      }

      assertTrue(cache.containsKey(myKey));
   }
}
