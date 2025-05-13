package org.infinispan.statetransfer;

import static java.lang.String.valueOf;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.testng.annotations.Test;

import jakarta.transaction.Status;

/**
 * Checks if the transactions are forward correctly to the new owners
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "statetransfer.TxDuringStateTransferTest")
@CleanupAfterMethod
public class TxDuringStateTransferTest extends MultipleCacheManagersTest {

   private static final String INITIAL_VALUE = "v1";
   private static final String FINAL_VALUE = "v2";

   public void testPut() throws Exception {
      performTest(Operation.PUT);
   }

   public void testRemove() throws Exception {
      performTest(Operation.REMOVE);
   }

   public void testReplace() throws Exception {
      performTest(Operation.REPLACE);
   }

   public void testConditionalPut() throws Exception {
      performTest(Operation.CONDITIONAL_PUT);
   }

   public void testConditionalRemove() throws Exception {
      performTest(Operation.CONDITIONAL_REMOVE);
   }

   public void testConditionalReplace() throws Exception {
      performTest(Operation.CONDITIONAL_REPLACE);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction()
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .useSynchronization(false)
            .recovery().disable();
      builder.clustering()
            .stateTransfer().fetchInMemoryState(true)
            .hash().numOwners(3);
      createClusteredCaches(4, TestDataSCI.INSTANCE, builder);
   }

   private void performTest(Operation operation) throws Exception {
      assertClusterSize("Wrong number of caches.", 4);
      final Object key = new MagicKey(cache(0), cache(1), cache(2));
      //init
      operation.init(cache(0), key);

      final EmbeddedTransactionManager transactionManager = (EmbeddedTransactionManager) tm(0);
      transactionManager.begin();
      operation.perform(cache(0), key);
      final EmbeddedTransaction transaction = transactionManager.getTransaction();
      transaction.runPrepare();
      assertEquals("Wrong transaction status before killing backup owner.",
                   Status.STATUS_PREPARED, transaction.getStatus());

      //now, we kill cache(1). the transaction is prepared in cache(1) and it should be forward to cache(3)
      killMember(1);

      assertEquals("Wrong transaction status after killing backup owner.",
                   Status.STATUS_PREPARED, transaction.getStatus());
      transaction.runCommit(false);

      for (Cache<Object, Object> cache : caches()) {
         //all the caches are owner
         operation.check(cache, key, valueOf(address(cache)));
      }
   }

   private enum Operation {
      PUT,
      REMOVE,
      REPLACE,
      CONDITIONAL_PUT,
      CONDITIONAL_REMOVE,
      CONDITIONAL_REPLACE;

      public final void init(Cache<Object, Object> cache, Object key) {
         if (this != CONDITIONAL_PUT) {
            cache.put(key, INITIAL_VALUE);
         }
      }

      public final void perform(Cache<Object, Object> cache, Object key) {
         switch (this) {
            case PUT:
               cache.put(key, FINAL_VALUE);
               break;
            case REMOVE:
               cache.remove(key);
               break;
            case REPLACE:
               cache.replace(key, FINAL_VALUE);
               break;
            case CONDITIONAL_PUT:
               cache.putIfAbsent(key, FINAL_VALUE);
               break;
            case CONDITIONAL_REMOVE:
               cache.remove(key, INITIAL_VALUE);
               break;
            case CONDITIONAL_REPLACE:
               cache.replace(key, INITIAL_VALUE, FINAL_VALUE);
               break;
         }
      }

      public final void check(Cache<Object, Object> cache, Object key, String cacheAddress) {
         //all the caches are owner. So, check in data container.
         DataContainer dataContainer = cache.getAdvancedCache().getDataContainer();
         if (this == REMOVE || this == CONDITIONAL_REMOVE) {
            assertFalse("Key was not removed in '" + cacheAddress + "'!", dataContainer.containsKey(key));
         } else {
            InternalCacheEntry entry = dataContainer.peek(key);
            assertNotNull("Cache '" + cacheAddress + "' does not contains entry!", entry);
            assertEquals("Cache '" + cacheAddress + "' has wrong value!", FINAL_VALUE, entry.getValue());
         }
      }
   }
}
