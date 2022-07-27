package org.infinispan.scattered;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.test.skip.SkipTestNG;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "scattered.SingleNodeScatteredTest")
public class SingleNodeScatteredTest extends ScatteredSyncFuncTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new SingleNodeScatteredTest().biasAcquisition(BiasAcquisition.NEVER),
            new SingleNodeScatteredTest().biasAcquisition(BiasAcquisition.ON_WRITE),
      };
   }

   public SingleNodeScatteredTest() {
      super();
      INIT_CLUSTER_SIZE = 1;
   }

   @Override
   public void testComputeFromNonOwner() {
      SkipTestNG.skipIf(true, "Single node test");
   }

   @Override
   public void testComputeIfAbsentFromNonOwner() {
      SkipTestNG.skipIf(true, "Single node test");
   }

   @Override
   public void testComputeIfPresentFromNonOwner() {
      SkipTestNG.skipIf(true, "Single node test");
   }

   @Override
   public void testConditionalRemoveFromNonOwner() {
      SkipTestNG.skipIf(true, "Single node test");
   }

   @Override
   public void testMergeFromNonOwner() {
      SkipTestNG.skipIf(true, "Single node test");
   }

   @Override
   public void testPutFromNonOwner() {
      SkipTestNG.skipIf(true, "Single node test");
   }

   @Override
   public void testRemoveFromNonOwner() {
      SkipTestNG.skipIf(true, "Single node test");
   }

   @Override
   public void testReplaceFromNonOwner() {
      SkipTestNG.skipIf(true, "Single node test");
   }

   @Override
   public void testConditionalReplaceFromNonOwner() {
      SkipTestNG.skipIf(true, "Single node test");
   }

   @Override
   public void testPutIfAbsentFromNonOwner() {
      SkipTestNG.skipIf(true, "Single node test");
   }

   @Override
   public void testPutAll() {
      Map<MagicKey, String> entries = new HashMap<>();
      for (int i = 0; i < 5; i++) {
         entries.put(new MagicKey(cache(0, cacheName)), String.valueOf(i));
      }

      cache(cacheName).putAll(entries);
      for (Map.Entry<MagicKey, String> entry: entries.entrySet()) {
         assertLocalValue(0, entry.getKey(), entry.getValue());
         assertOwnershipAndNonOwnership(entry.getKey(), false);
      }
   }

   @Override
   public void testCompute() {
      MagicKey key = new MagicKey(cache(cacheName));
      cache(cacheName).put(key, "a");

      assertEquals("ab", cache(cacheName).compute(key, (k, v) -> v + "b"));
      assertLocalValue(0, key, "ab");
      assertOwnershipAndNonOwnership(key, false);

      assertNull(cache(cacheName).compute(key, (k, v) -> "ab".equals(v) ? null : "unexpected"));
      assertLocalValue(0, key, null);
      assertOwnershipAndNonOwnership(key, false);

      MagicKey otherKey = new MagicKey(cache(cacheName));

      assertEquals("x", cache(cacheName).compute(otherKey, (k, v) -> v == null ? "x" : "unexpected"));
      assertLocalValue(0, otherKey, "x");
      assertOwnershipAndNonOwnership(otherKey, false);

      assertNull(cache(cacheName).compute(otherKey, (k, v) -> "x".equals(v) ? null : "unexpected"));
      assertLocalValue(0, otherKey, null);
      assertOwnershipAndNonOwnership(otherKey, false);

      assertEquals("y", cache(cacheName).compute(otherKey, (k, v) -> v == null ? "y" : "unexpected"));
      assertLocalValue(0, otherKey, "y");
      assertOwnershipAndNonOwnership(otherKey, false);
   }

   @Override
   public void testComputeIfPresent() {
      MagicKey key = new MagicKey(cache(cacheName));
      cache(cacheName).put(key, "a");

      assertLocalValue(0, key, "a");
      assertOwnershipAndNonOwnership(key, false);

      assertNull(cache(cacheName).computeIfPresent(key, (k, v) -> "a".equals(v) ? null : "unexpected"));
      assertLocalValue(0, key, null);
      assertOwnershipAndNonOwnership(key, false);

      MagicKey otherKey = new MagicKey(cache(cacheName));
      assertNull(cache(cacheName).computeIfPresent(otherKey, (k, v) -> "unexpected"));
      assertNoLocalValue(0, otherKey);
   }

   @Override
   public void testComputeIfAbsent() {
      MagicKey key = new MagicKey(cache(cacheName));
      cache(cacheName).put(key, "a");

      assertEquals("a", cache(cacheName).computeIfAbsent(key, k -> "b"));
      assertLocalValue(0, key, "a");
      assertOwnershipAndNonOwnership(key, false);

      MagicKey otherKey = new MagicKey(cache(0, cacheName));

      assertEquals("x", cache(cacheName).computeIfAbsent(otherKey, k -> "x"));
      assertLocalValue(0, otherKey, "x");
      assertOwnershipAndNonOwnership(otherKey, false);

      assertEquals("x", cache(0, cacheName).computeIfAbsent(otherKey, k -> null));
      assertLocalValue(0, otherKey, "x");
      assertOwnershipAndNonOwnership(otherKey, false);
   }

   @Override
   public void testMerge() {
      MagicKey key = new MagicKey(cache(cacheName));
      cache(cacheName).put(key, "a");

      assertEquals("ab", this.<Object, String>cache(cacheName).merge(key, "b", (o, n) -> o + n));
      assertLocalValue(0, key, "ab");
      assertOwnershipAndNonOwnership(key, false);

      assertNull(cache(cacheName).merge(key, "x", (o, n) -> "ab".equals(o) ? null : "unexpected"));
      assertLocalValue(0, key, null);
      assertOwnershipAndNonOwnership(key, false);

      MagicKey otherKey = new MagicKey(cache(0, cacheName));

      // on non-existent value, should work as putIfAbsent
      assertEquals("x", cache(cacheName).merge(otherKey, "x", (o, n) -> "unexpected"));
      assertLocalValue(0, otherKey, "x");
      assertOwnershipAndNonOwnership(otherKey, false);

      // removing from owner
      assertNull(cache(cacheName).merge(otherKey, "y", (o, n) -> "x".equals(o) ? null : "unexpected"));
      assertLocalValue(0, otherKey, null);
      assertOwnershipAndNonOwnership(otherKey, false);
   }

   @Override
   protected void assertOwnershipConsensus(String key) {
      List<Address> c1Owners = getCacheTopology(c1).getDistribution(key).writeOwners();
      Set<Address> keyOwners = new HashSet<>();
      for (Cache<Object, String> c : caches) {
         keyOwners.addAll(getCacheTopology(c).getDistribution(key).writeOwners());
      }

      assertEquals(c1Owners.size(), keyOwners.size());
      assertEquals(new HashSet<>(c1Owners), keyOwners);
   }

   private <K, V> Cache<K, V> cache(String cacheName) {
      return manager(0).getCache(cacheName);
   }
}
