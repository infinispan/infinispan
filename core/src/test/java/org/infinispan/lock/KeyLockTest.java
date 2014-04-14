package org.infinispan.lock;

import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

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

   public void testByteArrayStrippedLockTx() throws InterruptedException {
      doTest(CacheName.STRIPPED_LOCK_TX);
   }

   public void testByteArrayStrippedLockNonTx() throws InterruptedException {
      doTest(CacheName.STRIPPED_LOCK_NON_TX);
   }

   public void testByteArrayPerEntryLockTx() throws InterruptedException {
      doTest(CacheName.PER_ENTRY_LOCK_TX);
   }

   public void testByteArrayPerEntryLockNonTx() throws InterruptedException {
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

   private void doTest(CacheName cacheName) throws InterruptedException {
      final LockContainer<?> lockContainer = TestingUtil.extractComponent(cache(cacheName.name()), LockContainer.class);
      final Object lockOwner = new Object();
      AssertJUnit.assertNotNull(lockContainer.acquireLock(lockOwner, byteArray(), 10, TimeUnit.MILLISECONDS));
      AssertJUnit.assertTrue(lockContainer.isLocked(byteArray()));

      fork(new Runnable() {
         @Override
         public void run() {
            try {
               for (int i = 0; i < RETRIES; ++i) {
                  AssertJUnit.assertTrue(lockContainer.isLocked(byteArray()));
                  AssertJUnit.assertNull(lockContainer.acquireLock(new Object(), byteArray(), 10, TimeUnit.MILLISECONDS));
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
         }
      }, true);
   }

   private static byte[] byteArray() {
      return new byte[]{1};
   }

   private static enum CacheName {
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
