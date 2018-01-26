package org.infinispan.distribution;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.testng.annotations.Test;

/**
 * Non-transactional tests for asynchronous methods in a distributed
 * environment and a single owner.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.SingleOwnerAndAsyncMethodsTest")
public class SingleOwnerAndAsyncMethodsTest extends BaseDistFunctionalTest<Object, String> {

   public SingleOwnerAndAsyncMethodsTest() {
      INIT_CLUSTER_SIZE = 2;
      numOwners = 1;
      l1CacheEnabled = false;
   }

   public void testAsyncPut(Method m) throws Exception {
      Cache<Object, String> ownerCache = getOwner(k(m));
      ownerCache.put(k(m), v(m));
      CompletableFuture<String> f = ownerCache.putAsync(k(m), v(m, 1));
      assert f != null;
      assertEquals(v(m), f.get());
   }

   public void testAsyncGet(Method m) throws Exception {
      final String key = k(m);
      final String value = v(m);
      Cache<Object, String> ownerCache = getOwner(key);
      ownerCache.put(key, value);
      AdvancedCache<Object, String> nonOwnerCache = getNonOwner(key).getAdvancedCache();

      // Make the cache getAsync call go remote to verify it gets it correctly
      CompletableFuture<String> f = nonOwnerCache.getAsync(key);
      assert f != null;
      assert f.get().equals(value);

      f = nonOwnerCache.withFlags(Flag.SKIP_REMOTE_LOOKUP).getAsync(key);
      assert f != null;
      assert f.get() == null;

      f = nonOwnerCache.getAsync(key);
      assert f != null;
      assert f.get().equals(value);

      f = nonOwnerCache.withFlags(Flag.CACHE_MODE_LOCAL).getAsync(key);
      assert f != null;
      assert f.get() == null;

      f = nonOwnerCache.getAsync(key);
      assert f != null;
      assert f.get().equals(value);
   }

   public void testAsyncGetAll(Method m) throws Exception {
      final String k1 = k(m, 1);
      final String v1 = v(m, 1), v2 = v(m, 2);

      Cache<Object, String> ownerCache = getOwner(k1);
      AdvancedCache<Object, String> nonOwnerCache = getNonOwner(k1).getAdvancedCache();

      String k2;
      for (int counter = 2;; ++counter) {
         k2 = k(m, counter);
         if (getOwner(k2).equals(nonOwnerCache)) break;
         if (counter > 1000) {
            // unlucky, don't run the test further
            throw new IllegalStateException("Cannot find suitable key");
         }
      }

      Set<String> keys = new HashSet<>(Arrays.asList(k1, k2));
      Map<String, String> entries = new HashMap<>();
      entries.put(k1, v1);
      entries.put(k2, v2);
      ownerCache.put(k1, v1);
      nonOwnerCache.put(k2, v2);

      // Make the cache getAsync call go remote to verify it gets it correctly
      CompletableFuture<Map<Object, String>> f = nonOwnerCache.getAllAsync(keys);
      assertNotNull(f);
      assertEquals(entries, f.get());

      f = nonOwnerCache.withFlags(Flag.SKIP_REMOTE_LOOKUP).getAllAsync(keys);
      assertNotNull(f);
      assertEquals(Collections.singletonMap(k2, v2), f.get());

      f = nonOwnerCache.withFlags(Flag.SKIP_REMOTE_LOOKUP).getAllAsync(Collections.singleton(k1));
      assertNotNull(f);
      assertEquals(Collections.emptyMap(), f.get());

      f = nonOwnerCache.getAllAsync(keys);
      assertNotNull(f);
      assertEquals(entries, f.get());

      f = nonOwnerCache.withFlags(Flag.CACHE_MODE_LOCAL).getAllAsync(keys);
      assertNotNull(f);
      assertEquals(Collections.singletonMap(k2, v2), f.get());

      f = nonOwnerCache.withFlags(Flag.CACHE_MODE_LOCAL).getAllAsync(Collections.singleton(k1));
      assertNotNull(f);
      assertEquals(Collections.emptyMap(), f.get());

      f = nonOwnerCache.getAllAsync(keys);
      assertNotNull(f);
      assertEquals(entries, f.get());
   }

   public void testAsyncReplace(Method m) throws Exception {
      // Calling replaceAsync() on a cache that does not own the key will force
      // a remote get call to find out whether the key is associated with any
      // value.
      CompletableFuture<String> f = getOwner(k(m)).replaceAsync(k(m), v(m));
      assert f != null;
      // In this case k1 is not present in cache(1), so should return null
      assert f.get() == null;
      // Now let's put put the key in the owner cache and then verify
      // that a replace call from the non-owner cache resolves correctly.
      getOwner(k(m)).put(k(m), v(m));
      f = getNonOwner(k(m)).replaceAsync(k(m), v(m, 1));
      assert f != null;
      assert f.get().equals(v(m));
   }

   public void testAsyncGetThenPutOnSameNode(Method m) throws Exception {
      Cache<Object, String> ownerCache = getOwner(k(m));
      Cache<Object, String> nonOwnerCache = getNonOwner(k(m));
      ownerCache.put(k(m), v(m));
      // Make the cache getAsync call go remote to verify it gets it correctly
      CompletableFuture<String> f = nonOwnerCache.getAsync(k(m));
      assert f != null;
      assert f.get().equals(v(m));
      nonOwnerCache.put(k(m, 1), v(m, 1));
   }

   public void testParallelAsyncGets(Method m) throws Exception {
      getOwner(k(m, 1)).put(k(m, 1), v(m, 1));
      getOwner(k(m, 2)).put(k(m, 2), v(m, 2));
      getOwner(k(m, 3)).put(k(m, 3), v(m, 3));

      CompletableFuture<String> f1 = getNonOwner(k(m, 1)).getAsync(k(m, 1));
      CompletableFuture<String> f2 = getNonOwner(k(m, 2)).getAsync(k(m, 2));
      CompletableFuture<String> f3 = getNonOwner(k(m, 3)).getAsync(k(m, 3));

      assert f1 != null;
      assert f1.get().equals(v(m, 1));
      assert f2 != null;
      assert f2.get().equals(v(m, 2));
      assert f3 != null;
      assert f3.get().equals(v(m, 3));

      getNonOwner(k(m, 1)).put(k(m, 1), v(m, 11));
      getNonOwner(k(m, 2)).put(k(m, 2), v(m, 22));
      getNonOwner(k(m, 3)).put(k(m, 3), v(m, 33));

      f1 = getOwner(k(m, 1)).getAsync(k(m, 1));
      f2 = getOwner(k(m, 2)).getAsync(k(m, 2));
      f3 = getOwner(k(m, 3)).getAsync(k(m, 3));

      assert f1 != null;
      assert f1.get().equals(v(m, 11));
      assert f2 != null;
      assert f2.get().equals(v(m, 22));
      assert f3 != null;
      assert f3.get().equals(v(m, 33));
   }

   public void testLocalAsyncGet(Method m) throws Exception {
      Cache<Object, String> ownerCache = getOwner(k(m));
      ownerCache.put(k(m), v(m));
      CompletableFuture<String> f = getNonOwner(k(m)).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).getAsync(k(m));
      assert f != null;
      assert f.get() == null;
   }

   protected Cache<Object, String> getOwner(Object key) {
      return getOwners(key)[0];
   }

   protected Cache<Object, String> getNonOwner(Object key) {
      return getNonOwners(key)[0];
   }

   public Cache<Object, String>[] getOwners(Object key) {
      return getOwners(key, 1);
   }

   public Cache<Object, String>[] getNonOwners(Object key) {
      return getNonOwners(key, 1);
   }

}
