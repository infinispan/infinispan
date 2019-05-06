package org.infinispan.invalidation;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Test that global locks protect the shared store with pessimistic and optimistic locking.
 *
 * <p>See ISPN-10029</p>
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "invalidation.TxInvalidationLockingTest")
public class TxInvalidationLockingTest extends MultipleCacheManagersTest {
   private static final String KEY = "key";
   private static final String VALUE1 = "value1";
   private static final Object VALUE2 = "value2";
   private static final String PESSIMISTIC_CACHE = "pessimistic";
   private static final String OPTIMISTIC_CACHE = "optimistic";

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalConfig1 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfig1.defaultCacheName("local")
                  .globalState().configurationStorage(ConfigurationStorage.IMMUTABLE).disable();
      GlobalConfigurationBuilder globalConfig2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfig2.defaultCacheName("local")
                   .globalState().configurationStorage(ConfigurationStorage.IMMUTABLE).disable();
      ConfigurationBuilder localConfig = new ConfigurationBuilder();
      addClusterEnabledCacheManager(globalConfig1, localConfig);
      addClusterEnabledCacheManager(globalConfig2, localConfig);

      defineCache(PESSIMISTIC_CACHE, LockingMode.PESSIMISTIC);
      defineCache(OPTIMISTIC_CACHE, LockingMode.OPTIMISTIC);
      waitForClusterToForm(PESSIMISTIC_CACHE, OPTIMISTIC_CACHE);
   }

   private void defineCache(String cacheName, LockingMode pessimistic) {
      ConfigurationBuilder pessimisticConfig = buildConfig(TransactionMode.TRANSACTIONAL, pessimistic);
      manager(0).defineConfiguration(cacheName, pessimisticConfig.build());
      manager(1).defineConfiguration(cacheName, pessimisticConfig.build());
   }

   private ConfigurationBuilder buildConfig(TransactionMode transactionMode, LockingMode lockingMode) {
      ConfigurationBuilder cacheConfig = new ConfigurationBuilder();
      cacheConfig.clustering().cacheMode(CacheMode.INVALIDATION_SYNC)
                 .stateTransfer().fetchInMemoryState(false)
                 .transaction().transactionMode(transactionMode)
                 .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
                 .lockingMode(lockingMode)
                 .locking().isolationLevel(IsolationLevel.REPEATABLE_READ)
                 .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
                 .storeName(TxInvalidationLockingTest.class.getName())
                 .build();
      return cacheConfig;
   }

   public void testPessimisticWriteAcquiresGlobalLock() throws Exception {
      Future<Void> tx2Future;

      Cache<Object, Object> cache1 = cache(0, PESSIMISTIC_CACHE);
      tm(cache1).begin();
      try {
         Object initialValue = cache1.put(KEY, VALUE1);
         assertNull(initialValue);

         tx2Future = fork(() -> {
            AdvancedCache<Object, Object> cache2 = advancedCache(1, PESSIMISTIC_CACHE);
            tm(cache2).begin();
            try {
               Object value = cache2.put(KEY, VALUE2);
               assertEquals(VALUE1, value);
            } finally {
               tm(cache2).commit();
            }
         });

         Thread.sleep(10);
         assertFalse(tx2Future.isDone());

      } finally {
         tm(cache1).commit();
      }

      tx2Future.get();
      assertEquals(VALUE2, cache1.get(KEY));
   }

   public void testPessimisticForceWriteLockAcquiresGlobalLock() throws Exception {
      Future<Void> tx2Future;

      AdvancedCache<Object, Object> cache1 = advancedCache(0, PESSIMISTIC_CACHE);
      tm(cache1).begin();
      try {
         Object initialValue = cache1.withFlags(Flag.FORCE_WRITE_LOCK).get(KEY);
         assertNull(initialValue);

         tx2Future = fork(() -> {
            AdvancedCache<Object, Object> cache2 = advancedCache(1, PESSIMISTIC_CACHE);
            tm(cache2).begin();
            try {
               Object value = cache2.withFlags(Flag.FORCE_WRITE_LOCK).get(KEY);
               assertEquals(VALUE1, value);
               cache2.withFlags(Flag.IGNORE_RETURN_VALUES).put(KEY, VALUE2);
            } finally {
               tm(cache2).commit();
            }
         });

         Thread.sleep(10);
         assertFalse(tx2Future.isDone());

         cache1.put(KEY, VALUE1);
      } finally {
         tm(cache1).commit();
      }

      tx2Future.get();
      assertEquals(VALUE2, cache1.get(KEY));
   }

   public void testOptimisticPrepareAcquiresGlobalLock() throws Exception {
      CheckPoint checkPoint = new CheckPoint();
      Future<Void> tx2Future;

      Cache<Object, Object> cache1 = cache(0, OPTIMISTIC_CACHE);
      tm(cache1).begin();
      EmbeddedTransaction tx1 = null;
      try {
         Object initialValue = cache1.put(KEY, VALUE1);
         assertNull(initialValue);
         tx1 = (EmbeddedTransaction) tm(cache1).getTransaction();
         tx1.runPrepare();

         tx2Future = fork(() -> {
            AdvancedCache<Object, Object> cache2 = advancedCache(1, OPTIMISTIC_CACHE);
            tm(cache2).begin();
            try {
               assertNull(cache2.get(KEY));
               checkPoint.trigger("tx2_read");

               cache2.put(KEY, VALUE2);
            } finally {
               tm(cache2).commit();
            }
         });

         checkPoint.awaitStrict("tx2_read", 10, TimeUnit.SECONDS);

         Thread.sleep(10);
         assertFalse(tx2Future.isDone());
      } finally {
         if (tx1 != null) {
            tx1.runCommit(false);
         }
      }

      // No WriteSkewException
      tx2Future.get(30, TimeUnit.SECONDS);
      assertEquals(VALUE2, cache1.get(KEY));
   }

   public void testReadOnlyTransaction() throws Exception {
      // pessimistic - regular read
      AdvancedCache<Object, Object> pessimisticCache1 = advancedCache(0, PESSIMISTIC_CACHE);
      tm(pessimisticCache1).begin();
      try {
         Object initialValue = pessimisticCache1.get(KEY);
         assertNull(initialValue);
      } finally {
         tm(pessimisticCache1).commit();
      }

      // pessimistic - read with write lock
      tm(pessimisticCache1).begin();
      try {
         Object initialValue = pessimisticCache1.withFlags(Flag.FORCE_WRITE_LOCK).get(KEY);
         assertNull(initialValue);
      } finally {
         tm(pessimisticCache1).commit();
      }

      // optimistic
      AdvancedCache<Object, Object> optimisticCache1 = advancedCache(0, OPTIMISTIC_CACHE);
      tm(optimisticCache1).begin();
      try {
         Object initialValue = optimisticCache1.get(KEY);
         assertNull(initialValue);
      } finally {
         tm(optimisticCache1).commit();
      }
   }
}
