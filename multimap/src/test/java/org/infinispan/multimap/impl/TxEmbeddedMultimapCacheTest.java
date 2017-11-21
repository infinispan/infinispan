package org.infinispan.multimap.impl;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.JULIEN;
import static org.infinispan.multimap.impl.MultimapTestUtils.KOLDO;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;
import static org.infinispan.multimap.impl.MultimapTestUtils.assertMultimapCacheSize;
import static org.infinispan.multimap.impl.MultimapTestUtils.getTransactionManager;
import static org.infinispan.multimap.impl.MultimapTestUtils.putValuesOnMultimapCache;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.api.embedded.MultimapCacheManager;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.function.SerializablePredicate;
import org.testng.annotations.Test;

/**
 * Multimap Cache with transactions in single cache
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Test(groups = "functional", testName = "multimap.TxEmbeddedMultimapCacheTest")
public class TxEmbeddedMultimapCacheTest extends EmbeddedMultimapCacheTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single multimapCache instance
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
      c.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      MultimapCacheManager multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(cm);
      multimapCacheManager.defineConfiguration("test", c.build());
      multimapCache = multimapCacheManager.get("test");
      return cm;
   }

   public void testSizeInExplicitTx() throws SystemException, NotSupportedException {
      assertMultimapCacheSize(multimapCache, 0);

      TransactionManager tm1 = getTransactionManager(multimapCache);
      tm1.begin();
      try {
         putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN, OIHANA);
         assertMultimapCacheSize(multimapCache, 2);
      } finally {
         tm1.rollback();
      }

      assertMultimapCacheSize(multimapCache, 0);
   }

   public void testSizeInExplicitTxWithRemoveNonExistentAndPut() throws SystemException, NotSupportedException {
      assertMultimapCacheSize(multimapCache, 0);
      putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN);
      assertMultimapCacheSize(multimapCache, 1);

      TransactionManager tm1 = getTransactionManager(multimapCache);
      tm1.begin();
      try {
         await(multimapCache.remove("firstnames"));
         assertMultimapCacheSize(multimapCache, 1);
         putValuesOnMultimapCache(multimapCache, "firstnames", JULIEN, OIHANA, RAMON);
         assertMultimapCacheSize(multimapCache, 4);
      } finally {
         tm1.rollback();
      }
      assertMultimapCacheSize(multimapCache, 1);
   }

   public void testSizeInExplicitTxWithRemoveKeyValue() throws SystemException, NotSupportedException {
      assertMultimapCacheSize(multimapCache, 0);
      putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN, OIHANA);
      assertMultimapCacheSize(multimapCache, 2);

      TransactionManager tm1 = getTransactionManager(multimapCache);
      tm1.begin();
      try {
         await(multimapCache.remove(MultimapTestUtils.NAMES_KEY, JULIEN));
         assertMultimapCacheSize(multimapCache, 1);
      } finally {
         tm1.rollback();
      }
      assertMultimapCacheSize(multimapCache, 2);
   }

   public void testSizeInExplicitTxWithRemoveExistent() throws SystemException, NotSupportedException {
      assertMultimapCacheSize(multimapCache, 0);
      putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN);
      assertMultimapCacheSize(multimapCache, 1);

      TransactionManager tm1 = getTransactionManager(multimapCache);
      tm1.begin();
      try {
         putValuesOnMultimapCache(multimapCache, NAMES_KEY, OIHANA);
         assertMultimapCacheSize(multimapCache, 2);
         await(multimapCache.remove(MultimapTestUtils.NAMES_KEY));
         assertTrue(await(multimapCache.get(MultimapTestUtils.NAMES_KEY)).isEmpty());
      } finally {
         tm1.rollback();
      }
      assertMultimapCacheSize(multimapCache, 1);
   }

   public void testSizeInExplicitTxWithRemoveWithPredicate() throws SystemException, NotSupportedException {
      assertMultimapCacheSize(multimapCache, 0);
      putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN, OIHANA);
      assertMultimapCacheSize(multimapCache, 2);

      TransactionManager tm1 = getTransactionManager(multimapCache);
      tm1.begin();
      try {
         await(multimapCache.remove(v -> v.getName().contains("Ju"))
               .thenCompose(r1 -> multimapCache.get(NAMES_KEY))
               .thenAccept(values -> {
                        assertTrue(values.contains(OIHANA));
                        assertFalse(values.contains(JULIEN));
                     }
               ));
         assertMultimapCacheSize(multimapCache, 1);
      } finally {
         tm1.rollback();
      }

      assertMultimapCacheSize(multimapCache, 2);
   }

   public void testSizeInExplicitTxWithRemoveAllWithPredicate() throws SystemException, NotSupportedException {
      assertMultimapCacheSize(multimapCache, 0);
      putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN, OIHANA, KOLDO);
      assertMultimapCacheSize(multimapCache, 3);
      TransactionManager tm1 = getTransactionManager(multimapCache);
      tm1.begin();
      try {
         SerializablePredicate<Person> removePredicate = v -> v.getName().contains("ih") || v.getName().contains("ol");
         multimapCache.remove(removePredicate).thenAccept(r -> {
            assertMultimapCacheSize(multimapCache, 1);
         }).join();
      } finally {
         tm1.rollback();
      }
      assertMultimapCacheSize(multimapCache, 3);
   }

   public void testSizeInExplicitTxWithModification() throws SystemException, NotSupportedException {
      assertMultimapCacheSize(multimapCache, 0);
      putValuesOnMultimapCache(multimapCache, NAMES_KEY, OIHANA);
      assertMultimapCacheSize(multimapCache, 1);

      TransactionManager tm1 = getTransactionManager(multimapCache);
      tm1.begin();
      try {
         putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN);
         putValuesOnMultimapCache(multimapCache, "morenames", RAMON);
         assertMultimapCacheSize(multimapCache, 3);
      } finally {
         tm1.rollback();
      }

      assertMultimapCacheSize(multimapCache, 1);
   }

   public void testContainsMethodsInExplicitTxWithModification() throws SystemException, NotSupportedException {
      TransactionManager tm1 = getTransactionManager(multimapCache);
      tm1.begin();

      await(multimapCache.containsKey(NAMES_KEY).thenAccept(c -> assertFalse(c)));
      await(multimapCache.containsValue(JULIEN).thenAccept(c -> assertFalse(c)));
      await(multimapCache.containsEntry(NAMES_KEY, JULIEN).thenAccept(c -> assertFalse(c)));

      try {
         putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN);
         await(multimapCache.containsKey(NAMES_KEY).thenAccept(c -> assertTrue(c)));
         await(multimapCache.containsValue(JULIEN).thenAccept(c -> assertTrue(c)));
         await(multimapCache.containsEntry(NAMES_KEY, JULIEN).thenAccept(c -> assertTrue(c)));
      } finally {
         tm1.rollback();
      }

      await(multimapCache.containsKey(NAMES_KEY).thenAccept(c -> assertFalse(c)));
      await(multimapCache.containsValue(JULIEN).thenAccept(c -> assertFalse(c)));
      await(multimapCache.containsEntry(NAMES_KEY, JULIEN).thenAccept(c -> assertFalse(c)));
   }
}
