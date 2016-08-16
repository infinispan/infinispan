package org.infinispan.lock;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.impl.LockContainer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests if the same lock is used for the same key.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "unit", testName = "lock.KeyLockTest")
@CleanupAfterTest
public class KeyLockTest extends SingleCacheManagerTest {

   private static final int RETRIES = 100;

   public void testByteArrayStrippedLockTx() throws Exception {
      doTest(CacheName.STRIPPED_LOCK_TX);
   }

   public void testByteArrayStrippedLockNonTx() throws Exception {
      doTest(CacheName.STRIPPED_LOCK_NON_TX);
   }

   public void testByteArrayPerEntryLockTx() throws Exception {
      doTest(CacheName.PER_ENTRY_LOCK_TX);
   }

   public void testByteArrayPerEntryLockNonTx() throws Exception {
      doTest(CacheName.PER_ENTRY_LOCK_NON_TX);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL);
      builder.locking().lockAcquisitionTimeout(100, TimeUnit.MILLISECONDS);
      builder.dataContainer().keyEquivalence(ByteArrayEquivalence.INSTANCE);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      for (CacheName cacheName : CacheName.values()) {
         cacheName.configure(builder);
         cacheManager.defineConfiguration(cacheName.name(), builder.build());
      }
      return cacheManager;
   }

   private void doTest(CacheName cacheName) throws Exception {
      final LockContainer lockContainer = TestingUtil.extractComponent(cache(cacheName.name()), LockContainer.class);
      final Object lockOwner = new Object();
      try {
         lockContainer.acquire(byteArray(), lockOwner, 10, TimeUnit.MILLISECONDS).lock();
      } catch (InterruptedException | TimeoutException e) {
         AssertJUnit.fail();
      }
      AssertJUnit.assertTrue(lockContainer.isLocked(byteArray()));

      fork(() -> {
         for (int i = 0; i < RETRIES; ++i) {
            AssertJUnit.assertTrue(lockContainer.isLocked(byteArray()));
            try {
               lockContainer.acquire(byteArray(), new Object(), 10, TimeUnit.MILLISECONDS).lock();
               AssertJUnit.fail();
            } catch (InterruptedException | TimeoutException e) {
               //expected
            }
         }
         return null;
      }).get(10, TimeUnit.SECONDS);
   }

   private static byte[] byteArray() {
      return new byte[]{1};
   }

   private enum CacheName {
      STRIPPED_LOCK_TX {
         @Override
         void configure(ConfigurationBuilder builder) {
            builder.locking().useLockStriping(true);
            builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
         }
      },
      STRIPPED_LOCK_NON_TX {
         @Override
         void configure(ConfigurationBuilder builder) {
            builder.locking().useLockStriping(true);
            builder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
         }
      },
      PER_ENTRY_LOCK_TX {
         @Override
         void configure(ConfigurationBuilder builder) {
            builder.locking().useLockStriping(false);
            builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
         }
      },
      PER_ENTRY_LOCK_NON_TX {
         @Override
         void configure(ConfigurationBuilder builder) {
            builder.locking().useLockStriping(false);
            builder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
         }
      };

      abstract void configure(ConfigurationBuilder builder);
   }
}
