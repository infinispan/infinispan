package org.infinispan.container.versioning;

import static org.testng.Assert.assertNull;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * This test checks behaivour of versioned caches
 * when executing conditional operations.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(testName = "container.versioning.VersionedConditionalOperationsTest", groups = "functional")
public class VersionedConditionalOperationsTest extends MultipleCacheManagersTest {

   protected static final String KEY_1 = "key_1";
   protected static final String KEY_2 = "key_2";
   protected static final String VALUE_1 = "value_1";
   protected static final String VALUE_2 = "value_2";
   protected static final String VALUE_3 = "value_3";
   protected static final String VALUE_4 = "value_4";

   protected final int clusterSize;
   protected final CacheMode mode;
   protected final boolean syncCommit;

   public VersionedConditionalOperationsTest() {
      this(2, CacheMode.REPL_SYNC, true);
   }

   protected VersionedConditionalOperationsTest(
         int clusterSize, CacheMode mode, boolean syncCommit) {
      this.clusterSize = clusterSize;
      this.mode = mode;
      this.syncCommit = syncCommit;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(mode, true);
      dcc.transaction().syncCommitPhase(syncCommit).syncRollbackPhase(syncCommit);
      dcc.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true);
      dcc.versioning().enable().scheme(VersioningScheme.SIMPLE);
      createCluster(dcc, clusterSize);
      waitForClusterToForm();
   }

   public void testPutIfAbsent() {
      assertEmpty(KEY_1, KEY_2);

      cache(1).put(KEY_1, VALUE_1);
      assertCacheValue(1, KEY_1, VALUE_1);

      cache(0).putIfAbsent(KEY_1, VALUE_2);
      assertCacheValue(0, KEY_1, VALUE_1);

      cache(1).put(KEY_1, VALUE_3);
      assertCacheValue(1, KEY_1, VALUE_3);

      cache(0).putIfAbsent(KEY_1, VALUE_4);
      assertCacheValue(0, KEY_1, VALUE_3);

      cache(0).putIfAbsent(KEY_2, VALUE_1);
      assertCacheValue(0, KEY_2, VALUE_1);

      assertNoTransactions();
   }

   public void testRemoveIfPresent() {
      assertEmpty(KEY_1);

      cache(0).put(KEY_1, VALUE_1);
      cache(1).put(KEY_1, VALUE_2);
      assertCacheValue(1, KEY_1, VALUE_2);

      cache(0).remove(KEY_1, VALUE_1);
      assertCacheValue(0, KEY_1, VALUE_2);

      cache(0).remove(KEY_1, VALUE_2);
      assertCacheValue(0, KEY_1, null);

      assertNoTransactions();
   }

   public void testReplaceWithOldVal() {
      assertEmpty(KEY_1);

      cache(1).put(KEY_1, VALUE_1);
      assertCacheValue(1, KEY_1, VALUE_1);

      cache(0).put(KEY_1, VALUE_2);
      assertCacheValue(0, KEY_1, VALUE_2);

      cache(0).replace(KEY_1, VALUE_3, VALUE_4);
      assertCacheValue(0, KEY_1, VALUE_2);

      cache(0).replace(KEY_1, VALUE_2, VALUE_4);
      assertCacheValue(0, KEY_1, VALUE_4);

      assertNoTransactions();
   }

   protected void assertEmpty(Object... keys) {
      for (Cache cache : caches()) {
         for (Object key : keys) {
            assertNull(cache.get(key));
         }
      }
   }

   //originatorIndex == cache which executed the transaction
   protected void assertCacheValue(int originatorIndex, Object key, Object value) {
      for (int index = 0; index < caches().size(); ++index) {
         if ((index == originatorIndex && mode.isSynchronous()) ||
               (index != originatorIndex && syncCommit)) {
            assertEquals(index, key, value);
         } else {
            assertEventuallyEquals(index, key, value);
         }
      }

   }

   private void assertEquals(int index, Object key, Object value) {
      assert value == null
            ? value == cache(index).get(key)
            : value.equals(cache(index).get(key));
   }
}