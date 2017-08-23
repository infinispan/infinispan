package org.infinispan.api.mvcc;

import static org.testng.AssertJUnit.assertEquals;

import java.util.function.Consumer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.eventually.Condition;
import org.infinispan.test.eventually.Eventually;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionProtocol;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.PutForExternalReadLockCleanupTest")
@CleanupAfterMethod
public class PutForExternalReadLockCleanupTest extends MultipleCacheManagersTest {

   private static final String VALUE = "v";
   private static final Consumer<ConfigurationBuilder> ENABLE_L1 = c -> c.clustering().l1().enable();

   private String name;
   private Consumer<ConfigurationBuilder> amendConfiguration;

   @Override
   public Object[] factory() {
      return new Object[] {
         new PutForExternalReadLockCleanupTest("NonTx").transactional(false),
         new PutForExternalReadLockCleanupTest("Optimistic").transactional(true).lockingMode(LockingMode.OPTIMISTIC),
         new PutForExternalReadLockCleanupTest("Pessimistic").transactional(true).lockingMode(LockingMode.PESSIMISTIC),
         new PutForExternalReadLockCleanupTest("TotalOrder").transactional(true).totalOrder(true),
         new PutForExternalReadLockCleanupTest("NonTxL1", ENABLE_L1).transactional(false),
         new PutForExternalReadLockCleanupTest("OptimisticL1", ENABLE_L1).transactional(true).lockingMode(LockingMode.OPTIMISTIC),
         new PutForExternalReadLockCleanupTest("PessimisticL1", ENABLE_L1).transactional(true).lockingMode(LockingMode.PESSIMISTIC),
         new PutForExternalReadLockCleanupTest("TotalOrderL1", ENABLE_L1).transactional(true).totalOrder(true),
      };
   }

   public PutForExternalReadLockCleanupTest() {}

   private PutForExternalReadLockCleanupTest(String name) {
      this(name, c -> {});
   }

   private PutForExternalReadLockCleanupTest(String name, Consumer<ConfigurationBuilder> amendConfiguration) {
      this.name = name;
      this.amendConfiguration = amendConfiguration;
   }

   @Override
   protected String parameters() {
      return "[" + name + "]";
   }

   public void testLockCleanupOnBackup() {
      doTest(false);
   }

   public void testLockCleanuponOwner() {
      doTest(true);
   }

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, transactional);
      c.clustering().hash().numSegments(10).numOwners(1);
      c.clustering().l1().disable();
      if (totalOrder != null && totalOrder.booleanValue()) {
         c.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
      }
      amendConfiguration.accept(c);
      createClusteredCaches(2, c);
   }

   private void doTest(boolean owner) {
      final Cache<MagicKey, String> cache1 = cache(0);
      final Cache<MagicKey, String> cache2 = cache(1);
      final MagicKey magicKey = new MagicKey(cache1);

      if (owner) {
         cache1.putForExternalRead(magicKey, VALUE);
      } else {
         cache2.putForExternalRead(magicKey, VALUE);
      }

      Eventually.eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache1.containsKey(magicKey) && cache2.containsKey(magicKey);
         }
      });
      assertEquals(VALUE, cache1.get(magicKey));
      assertEquals(VALUE, cache2.get(magicKey));
      assertNotLocked(magicKey);
   }
}
