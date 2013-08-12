package org.infinispan.stats.simple;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.stats.wrappers.ExtendedStatisticInterceptor;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertNull;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.LocalExtendedStatisticTest")
public class LocalExtendedStatisticTest extends SingleCacheManagerTest {

   private static final String KEY_1 = "key_1";
   private static final String KEY_2 = "key_2";
   private static final String KEY_3 = "key_3";
   private static final String VALUE_1 = "value_1";
   private static final String VALUE_2 = "value_2";
   private static final String VALUE_3 = "value_3";
   private static final String VALUE_4 = "value_4";

   public void testPut() {
      assertEmpty(KEY_1, KEY_2, KEY_3);
      ExtendedStatisticInterceptor statisticInterceptor = getExtendedStatistic(cache);

      cache.put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put(KEY_2, VALUE_2);
      map.put(KEY_3, VALUE_3);

      cache.putAll(map);

      assertCacheValue(KEY_1, VALUE_1);
      assertCacheValue(KEY_2, VALUE_2);
      assertCacheValue(KEY_3, VALUE_3);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor.getCacheStatisticManager().hasPendingTransactions());
   }

   public void removeTest() {
      assertEmpty(KEY_1);
      ExtendedStatisticInterceptor statisticInterceptor = getExtendedStatistic(cache);

      cache.put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache.remove(KEY_1);

      assertCacheValue(KEY_1, null);

      cache.put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache.remove(KEY_1);

      assertCacheValue(KEY_1, null);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor.getCacheStatisticManager().hasPendingTransactions());
   }

   public void testPutIfAbsent() {
      assertEmpty(KEY_1, KEY_2);
      ExtendedStatisticInterceptor statisticInterceptor = getExtendedStatistic(cache);

      cache.put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache.putIfAbsent(KEY_1, VALUE_2);

      assertCacheValue(KEY_1, VALUE_1);

      cache.put(KEY_1, VALUE_3);

      assertCacheValue(KEY_1, VALUE_3);

      cache.putIfAbsent(KEY_1, VALUE_4);

      assertCacheValue(KEY_1, VALUE_3);

      cache.putIfAbsent(KEY_2, VALUE_1);

      assertCacheValue(KEY_2, VALUE_1);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor.getCacheStatisticManager().hasPendingTransactions());
   }

   public void testRemoveIfPresent() {
      assertEmpty(KEY_1);
      ExtendedStatisticInterceptor statisticInterceptor = getExtendedStatistic(cache);

      cache.put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache.put(KEY_1, VALUE_2);

      assertCacheValue(KEY_1, VALUE_2);

      cache.remove(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_2);

      cache.remove(KEY_1, VALUE_2);

      assertCacheValue(KEY_1, null);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor.getCacheStatisticManager().hasPendingTransactions());
   }

   public void testClear() {
      assertEmpty(KEY_1);
      ExtendedStatisticInterceptor statisticInterceptor = getExtendedStatistic(cache);

      cache.put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache.clear();

      assertCacheValue(KEY_1, null);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor.getCacheStatisticManager().hasPendingTransactions());
   }

   public void testReplace() {
      assertEmpty(KEY_1);
      ExtendedStatisticInterceptor statisticInterceptor = getExtendedStatistic(cache);

      cache.put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      Assert.assertEquals(cache.replace(KEY_1, VALUE_2), VALUE_1);

      assertCacheValue(KEY_1, VALUE_2);

      cache.put(KEY_1, VALUE_3);

      assertCacheValue(KEY_1, VALUE_3);

      cache.replace(KEY_1, VALUE_3);

      assertCacheValue(KEY_1, VALUE_3);

      cache.put(KEY_1, VALUE_4);

      assertCacheValue(KEY_1, VALUE_4);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor.getCacheStatisticManager().hasPendingTransactions());
   }

   public void testReplaceWithOldVal() {
      assertEmpty(KEY_1);
      ExtendedStatisticInterceptor statisticInterceptor = getExtendedStatistic(cache);

      cache.put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache.put(KEY_1, VALUE_2);

      assertCacheValue(KEY_1, VALUE_2);

      cache.replace(KEY_1, VALUE_3, VALUE_4);

      assertCacheValue(KEY_1, VALUE_2);

      cache.replace(KEY_1, VALUE_2, VALUE_4);

      assertCacheValue(KEY_1, VALUE_4);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor.getCacheStatisticManager().hasPendingTransactions());
   }

   public void testRemoveUnexistingEntry() {
      assertEmpty(KEY_1);
      ExtendedStatisticInterceptor statisticInterceptor = getExtendedStatistic(cache);

      cache.remove(KEY_1);

      assertCacheValue(KEY_1, null);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor.getCacheStatisticManager().hasPendingTransactions());
   }

//   @Override
//   protected void createCacheManagers() throws Throwable {
//      for (int i = 0; i < 2; ++i) {
//         ConfigurationBuilder builder = getDefaultClusteredCacheConfig(mode, true);
//         builder.transaction().syncCommitPhase(sync2ndPhase).syncRollbackPhase(sync2ndPhase);
//         if (totalOrder) {
//            builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
//         }
//         builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(writeSkew);
//         builder.clustering().hash().numOwners(1);
//         if (writeSkew) {
//            builder.versioning().enable().scheme(VersioningScheme.SIMPLE);
//         }
//         builder.transaction().recovery().disable();
//         builder.customInterceptors().addInterceptor().interceptor(new ExtendedStatisticInterceptor())
//               .position(InterceptorConfiguration.Position.FIRST);
//         addClusterEnabledCacheManager(builder);
//      }
//      waitForClusterToForm();
//   }

   protected void assertEmpty(Object... keys) {
      for (Object key : keys) {
         assertNull(cache.get(key));
      }
   }

   protected void assertCacheValue(Object key, Object value) {
      Assert.assertEquals(cache.get(key), value);

   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      builder.transaction().recovery().disable();
      builder.customInterceptors().addInterceptor().interceptor(new ExtendedStatisticInterceptor())
            .position(InterceptorConfiguration.Position.FIRST);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   private ExtendedStatisticInterceptor getExtendedStatistic(Cache<?, ?> cache) {
      for (CommandInterceptor commandInterceptor : cache.getAdvancedCache().getInterceptorChain()) {
         if (commandInterceptor instanceof ExtendedStatisticInterceptor) {
            ((ExtendedStatisticInterceptor) commandInterceptor).resetStatistics();
            return (ExtendedStatisticInterceptor) commandInterceptor;
         }
      }
      return null;
   }
}
