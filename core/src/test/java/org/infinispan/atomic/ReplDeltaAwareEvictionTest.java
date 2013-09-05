package org.infinispan.atomic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
@Test(groups = "functional", testName = "atomic.ReplDeltaAwareEvictionTest")
@CleanupAfterMethod
public class ReplDeltaAwareEvictionTest extends LocalDeltaAwareEvictionTest {

   public ReplDeltaAwareEvictionTest() {
      txEnabled = true;
   }

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true, true);
      builder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.PESSIMISTIC)
            .transactionManagerLookup(new JBossStandaloneJTAManagerLookup())
            .eviction().maxEntries(1).strategy(EvictionStrategy.LRU)
            .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .fetchPersistentState(false);

      addClusterEnabledCacheManager(builder);

      builder.persistence().clearStores()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class).fetchPersistentState(false);

      addClusterEnabledCacheManager(builder);

      waitForClusterToForm();
   }

   @Override
   public void testDeltaAware() throws Exception {
      test(createDeltaAwareAccessor(), 0, 1);
   }

   public void testDeltaAware2() throws Exception {
      test(createDeltaAwareAccessor(), 1, 0);
   }

   @Override
   public void testAtomicMap() throws Exception {
      test(createAtomicMapAccessor(), 0, 1);
   }

   public void testAtomicMap2() throws Exception {
      test(createAtomicMapAccessor(), 1, 0);
   }

   @Override
   public void testFineGrainedAtomicMap() throws Exception {
      test(createFineGrainedAtomicMapAccessor(), 0, 1);
   }

   public void testFineGrainedAtomicMap2() throws Exception {
      test(createFineGrainedAtomicMapAccessor(), 1, 0);
   }
}
