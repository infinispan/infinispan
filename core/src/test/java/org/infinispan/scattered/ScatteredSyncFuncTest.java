package org.infinispan.scattered;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.DistSyncFuncTest;
import org.infinispan.distribution.MagicKey;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "scattered.ScatteredSyncFuncTest")
public class ScatteredSyncFuncTest extends DistSyncFuncTest {
   @Override
   public Object[] factory() {
      return new Object[] {
            new ScatteredSyncFuncTest().biasAcquisition(BiasAcquisition.NEVER),
            new ScatteredSyncFuncTest().biasAcquisition(BiasAcquisition.ON_WRITE),
      };
   }

   public ScatteredSyncFuncTest() {
      cacheMode = CacheMode.SCATTERED_SYNC;
      numOwners = 1;
      l1CacheEnabled = false;
   }

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder builder = super.buildConfiguration();
      builder.clustering().biasAcquisition(biasAcquisition);
      return builder;
   }

   @Override
   protected void assertOwnershipAndNonOwnership(Object key, boolean allowL1) {
      Utils.assertOwnershipAndNonOwnership(caches, key);
   }

   public void testPutAll() {
      Map<MagicKey, String> nonOwner = new HashMap<>();
      for (int i = 0; i < 5; i++) {
         nonOwner.put(new MagicKey(cache(0, cacheName)), String.valueOf(i));
      }

      cache(2, cacheName).putAll(nonOwner);
      for (Map.Entry<MagicKey, String> entry: nonOwner.entrySet()) {
         assertLocalValue(0, entry.getKey(), entry.getValue());
         assertLocalValue(2, entry.getKey(), entry.getValue());
         assertOwnershipAndNonOwnership(entry.getKey(), false);
      }

      Map<MagicKey, String> fromOwner = new HashMap<>();
      for (int i = 0; i < 5; i++) {
         fromOwner.put(new MagicKey(cache(0, cacheName)), String.valueOf(i));
      }
      cache(0, cacheName).putAll(fromOwner);

      // We don't know which node is the backup.
      for (Map.Entry<MagicKey, String> entry: fromOwner.entrySet()) {
         assertLocalValue(0, entry.getKey(), entry.getValue());
         assertOwnershipAndNonOwnership(entry.getKey(), false);
      }
   }

   public void testCompute() {
      MagicKey key = new MagicKey(cache(0, cacheName));
      cache(1, cacheName).put(key, "a");
      // from non-owner and non-last writer
      assertEquals("ab", cache(2, cacheName).compute(key, (k, v) -> v + "b"));
      assertLocalValue(0, key, "ab");
      assertLocalValue(1, key, "a");
      assertLocalValue(2, key, "ab");
      assertOwnershipAndNonOwnership(key, false);

      // from owner
      assertEquals("abc", cache(0, cacheName).compute(key, (k, v) -> v + "c"));
      assertLocalValue(0, key, "abc");
      // we don't know which node become backup
      assertOwnershipAndNonOwnership(key, false);

      // removing from non-owner
      assertEquals(null, cache(1, cacheName).compute(key, (k, v) -> "abc".equals(v) ? null : "unexpected"));
      assertLocalValue(0, key, null);
      assertLocalValue(1, key, null);
      assertLocalValue(2, key, "ab");
      assertOwnershipAndNonOwnership(key, false);

      MagicKey otherKey = new MagicKey(cache(0, cacheName));

      // on non-existent value, non-owner
      assertEquals("x", cache(1, cacheName).compute(otherKey, (k, v) -> v == null ? "x" : "unexpected"));
      assertLocalValue(0, otherKey, "x");
      assertLocalValue(1, otherKey, "x");
      assertNoLocalValue(2, otherKey);
      assertOwnershipAndNonOwnership(otherKey, false);

      // removing from owner
      assertEquals(null, cache(0, cacheName).compute(otherKey, (k, v) -> "x".equals(v) ? null : "unexpected"));
      assertLocalValue(0, otherKey, null);
      // we don't know which node became backup of the tombstone
      assertOwnershipAndNonOwnership(otherKey, false);

      // on tombstone, from owner
      assertEquals("y", cache(0, cacheName).compute(otherKey, (k, v) -> v == null ? "y" : "unexpected"));
      assertLocalValue(0, otherKey, "y");
      assertOwnershipAndNonOwnership(otherKey, false);
   }

   public void testComputeIfPresent() {
      MagicKey key = new MagicKey(cache(0, cacheName));
      cache(1, cacheName).put(key, "a");

      // on non-owner and non-last-writer
      assertEquals("ab", cache(2, cacheName).computeIfPresent(key, (k, v) -> v + "b"));
      assertLocalValue(0, key, "ab");
      assertLocalValue(1, key, "a");
      assertLocalValue(2, key, "ab");
      assertOwnershipAndNonOwnership(key, false);

      // from owner
      assertEquals("abc", cache(0, cacheName).computeIfPresent(key, (k, v) -> v + "c"));
      assertLocalValue(0, key, "abc");
      // we don't know which node become backup
      assertOwnershipAndNonOwnership(key, false);

      // removing from non-owner
      assertEquals(null, cache(1, cacheName).computeIfPresent(key, (k, v) -> "abc".equals(v) ? null : "unexpected"));
      assertLocalValue(0, key, null);
      assertLocalValue(1, key, null);
      assertLocalValue(2, key, "ab");
      assertOwnershipAndNonOwnership(key, false);

      // on tombstone from owner
      assertEquals(null, cache(0, cacheName).computeIfPresent(key, (k, v) -> "unexpected"));
      assertLocalValue(0, key, null);
      assertLocalValue(1, key, null);
      assertLocalValue(2, key, "ab");
      assertOwnershipAndNonOwnership(key, false);

      MagicKey otherKey = new MagicKey(cache(0, cacheName));

      // on non-existent value, non-owner
      assertEquals(null, cache(1, cacheName).computeIfPresent(otherKey, (k, v) -> "unexpected"));
      assertNoLocalValue(0, otherKey);
      assertNoLocalValue(1, otherKey);
      assertNoLocalValue(2, otherKey);
      // cannot use assertOwnershipAndNonOwnership: no entry should be in any of the caches
   }

   public void testComputeIfAbsent() {
      MagicKey key = new MagicKey(cache(0, cacheName));
      cache(1, cacheName).put(key, "a");

      // from non-owner, non-last writer
      assertEquals("a", cache(2, cacheName).computeIfAbsent(key, k -> "b"));
      assertLocalValue(0, key, "a");
      assertLocalValue(1, key, "a");
      assertNoLocalValue(2, key);
      assertEquals("a", cache(2, cacheName).get(key));
      assertOwnershipAndNonOwnership(key, false);

      // from non-owner, last writer
      assertEquals("a", cache(1, cacheName).computeIfAbsent(key, k -> "c"));
      assertLocalValue(0, key, "a");
      assertLocalValue(1, key, "a");
      assertNoLocalValue(2, key);
      assertEquals("a", cache(2, cacheName).get(key));
      assertOwnershipAndNonOwnership(key, false);

      // from owner
      assertEquals("a", cache(2, cacheName).computeIfAbsent(key, k -> "d"));
      assertLocalValue(0, key, "a");
      assertLocalValue(1, key, "a");
      assertNoLocalValue(2, key);
      assertEquals("a", cache(2, cacheName).get(key));
      assertOwnershipAndNonOwnership(key, false);

      MagicKey otherKey = new MagicKey(cache(0, cacheName));

      // on non-existent value from non-owner
      assertEquals("x", cache(1, cacheName).computeIfAbsent(otherKey, k -> "x"));
      assertLocalValue(0, otherKey, "x");
      assertLocalValue(1, otherKey, "x");
      assertNoLocalValue(2, otherKey);
      assertOwnershipAndNonOwnership(otherKey, false);

      // on existent value from non-owner, non-last-writer
      assertEquals("x", cache(2, cacheName).computeIfAbsent(otherKey, k -> "y"));
      assertLocalValue(0, otherKey, "x");
      assertLocalValue(1, otherKey, "x");
      assertNoLocalValue(2, otherKey);
      assertOwnershipAndNonOwnership(otherKey, false);

      // removal on owner should do nothing
      assertEquals("x", cache(0, cacheName).computeIfAbsent(otherKey, k -> null));
      assertLocalValue(0, otherKey, "x");
      assertLocalValue(1, otherKey, "x");
      assertNoLocalValue(2, otherKey);
      assertOwnershipAndNonOwnership(otherKey, false);

      // removal on non-owner should do nothing
      assertEquals("x", cache(1, cacheName).computeIfAbsent(otherKey, k -> null));
      assertLocalValue(0, otherKey, "x");
      assertLocalValue(1, otherKey, "x");
      assertNoLocalValue(2, otherKey);
      assertOwnershipAndNonOwnership(otherKey, false);

      // removal on non-owner, non-last-writer should do nothing
      assertEquals("x", cache(2, cacheName).computeIfAbsent(otherKey, k -> null));
      assertLocalValue(0, otherKey, "x");
      assertLocalValue(1, otherKey, "x");
      assertNoLocalValue(2, otherKey);
      assertOwnershipAndNonOwnership(otherKey, false);
   }

   public void testMerge() {
      MagicKey key = new MagicKey(cache(0, cacheName));
      cache(1, cacheName).put(key, "a");

      // from non-owner and non-last writer
      assertEquals("ab", this.<Object, String>cache(2, cacheName).merge(key, "b", (o, n) -> o + n));
      assertLocalValue(0, key, "ab");
      assertLocalValue(1, key, "a");
      assertLocalValue(2, key, "ab");
      assertOwnershipAndNonOwnership(key, false);

      // from owner
      assertEquals("abc", this.<Object, String>cache(0, cacheName).merge(key, "c", (o, n) -> o + n));
      assertLocalValue(0, key, "abc");
      // we don't know which node become backup
      assertOwnershipAndNonOwnership(key, false);

      // removing from non-owner
      assertEquals(null, cache(1, cacheName).merge(key, "x", (o, n) -> "abc".equals(o) ? null : "unexpected"));
      assertLocalValue(0, key, null);
      assertLocalValue(1, key, null);
      assertLocalValue(2, key, "ab");
      assertOwnershipAndNonOwnership(key, false);

      MagicKey otherKey = new MagicKey(cache(0, cacheName));

      // on non-existent value, non-owner - should work as putIfAbsent
      assertEquals("x", cache(1, cacheName).merge(otherKey, "x", (o, n) -> "unexpected"));
      assertLocalValue(0, otherKey, "x");
      assertLocalValue(1, otherKey, "x");
      assertNoLocalValue(2, otherKey);
      assertOwnershipAndNonOwnership(otherKey, false);

      // removing from owner
      assertEquals(null, cache(0, cacheName).merge(otherKey, "y", (o, n) -> "x".equals(o) ? null : "unexpected"));
      assertLocalValue(0, otherKey, null);
      assertOwnershipAndNonOwnership(otherKey, false);

      // on tombstone, from owner
      assertEquals("z", cache(0, cacheName).merge(otherKey, "z", (o, n) -> "unexpected"));
      assertLocalValue(0, otherKey, "z");
      assertOwnershipAndNonOwnership(otherKey, false);
   }

   protected void assertNoLocalValue(int node, MagicKey key) {
      InternalCacheEntry<Object, Object> ice = cache(node, cacheName).getAdvancedCache().getDataContainer().get(key);
      assertEquals(null, ice);
   }

   protected void assertLocalValue(int node, MagicKey key, String expectedValue) {
      InternalCacheEntry<Object, Object> ice = cache(node, cacheName).getAdvancedCache().getDataContainer().get(key);
      assertNotNull(ice);
      assertEquals(expectedValue, ice.getValue());
   }
}
