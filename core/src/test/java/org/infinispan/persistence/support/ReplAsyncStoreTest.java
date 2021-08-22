package org.infinispan.persistence.support;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.4
 */
@Test(groups = "functional", testName = "persistence.support.ReplAsyncStoreTest")
public class ReplAsyncStoreTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = "testCache";

   private boolean shared;

   ReplAsyncStoreTest shared(boolean shared) {
      this.shared = shared;
      return this;
   }

   enum Op {
      SIZE {
         @Override
         void perform(MagicKey key, Cache<MagicKey, String> cache) {
            assertEquals(1, cache.size());
         }
      },
      KEY_ITERATOR {
         @Override
         void perform(MagicKey key, Cache<MagicKey, String> cache) {
            Iterator<?> iterator = cache.keySet().iterator();
            assertTrue(iterator.hasNext());
            assertEquals(key, iterator.next());
            assertFalse(iterator.hasNext());
         }
      },
      ENTRY_COLLECT {
         @Override
         void perform(MagicKey key, Cache<MagicKey, String> cache) {
            List<Map.Entry<MagicKey, String>> list = cache.entrySet().stream().collect(Collectors::toList);
            assertEquals(1, list.size());
            assertEquals(key, list.get(0).getKey());
         }
      }
      ;

      abstract void perform(MagicKey key, Cache<MagicKey, String> cache);
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new ReplAsyncStoreTest().shared(false),
            new ReplAsyncStoreTest().shared(true),
      };
   }

   @Override
   protected String[] parameterNames() {
      return new String[] { "shared" };
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[] { shared };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      org.infinispan.configuration.cache.ConfigurationBuilder
            cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);

      cfg.persistence()
            .addStore(DelayStore.ConfigurationBuilder.class)
               .storeName(shared ? ReplAsyncStoreTest.class.getName() : null)
               .shared(shared)
               .async().enable();

      createClusteredCaches(3, CACHE_NAME, TestDataSCI.INSTANCE, cfg);
      waitForClusterToForm(CACHE_NAME);
   }

   @DataProvider(name = "async-ops")
   public Object[][] asyncOperationProvider() {
      // Now smash all those ops with a true and false for using primary
      return Stream.of(Op.values()).flatMap(op ->
            Stream.of(true, false).map(bool -> new Object[] { bool, op }))
            .toArray(Object[][]::new);
   }

   @Test(dataProvider = "async-ops")
   public void testOperationAfterInsertAndEvict(boolean primary, Op consumer) {
      Cache<MagicKey, String> primaryOwner = cache(0, CACHE_NAME);
      Cache<MagicKey, String> backupOwner = cache(1, CACHE_NAME);
      MagicKey key = getKeyForCache(primaryOwner);

      // Delay the underlying store write
      DelayStore primaryStore = TestingUtil.getFirstStore(primaryOwner);
      primaryStore.delayBeforeModification(1);
      DelayStore backupStore = TestingUtil.getFirstStore(backupOwner);
      backupStore.delayBeforeModification(1);

      primaryOwner.put(key, "some-value");

      Cache<MagicKey, String> cacheToUse;

      if (primary) {
         cacheToUse = primaryOwner;
      } else {
         cacheToUse = backupOwner;
      }

      // Evict the key so it only exists in the store
      cacheToUse.evict(key);

      consumer.perform(key, cacheToUse);

      primaryStore.endDelay();
      backupStore.endDelay();
   }
}
