package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.distribution.MagicKey;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.infinispan.atomic.AtomicMapLookup.getAtomicMap;
import static org.infinispan.atomic.AtomicMapLookup.getFineGrainedAtomicMap;

/**
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "atomic.AtomicMapAPITest")
public class AtomicMapAPITest extends BaseAtomicHashMapAPITest {

   @SuppressWarnings("UnusedDeclaration")
   @Test(expectedExceptions = {IllegalArgumentException.class})
   public void testAtomicMapAfterFineGrainedAtomicMap(Method m) throws Exception {
      Cache<Object, Object> cache = cache(0, "atomic");
      final Object key = new MagicKey(m.getName(), cache);

      Map<String, String> map = getFineGrainedAtomicMap(cache, key);
      Map<String, String> map2 = getAtomicMap(cache, key);
   }

   @SuppressWarnings("UnusedDeclaration")
   @Test(expectedExceptions = {IllegalArgumentException.class})
   public void testAtomicMapBeforeFineGrainedAtomicMap(Method m) throws Exception {
      Cache<Object, Object> cache = cache(0, "atomic");
      final Object key = new MagicKey(m.getName(), cache);

      Map<String, String> map = getAtomicMap(cache, key);
      Map<String, String> map2 = getFineGrainedAtomicMap(cache, key);
   }

   @SuppressWarnings("UnusedDeclaration")
   @Test(expectedExceptions = {IllegalArgumentException.class})
   public void testAtomicMapAfterFineGrainedAtomicMapOnNonOwner(Method m) throws Exception {
      Cache<Object, Object> cache = cache(0, "atomic");
      if (cache.getCacheConfiguration().clustering().cacheMode().isReplicated()) {
         //no-op since everybody is an owner.
         throw new IllegalArgumentException();
      }
      final Object key = new MagicKey(m.getName(), cache(1, "atomic"));

      Map<String, String> map = getFineGrainedAtomicMap(cache, key);
      Map<String, String> map2 = getAtomicMap(cache, key);
   }

   @SuppressWarnings("UnusedDeclaration")
   @Test(expectedExceptions = {IllegalArgumentException.class})
   public void testAtomicMapBeforeFineGrainedAtomicMapOnNonOwner(Method m) throws Exception {
      Cache<Object, Object> cache = cache(0, "atomic");
      if (cache.getCacheConfiguration().clustering().cacheMode().isReplicated()) {
         //no-op since everybody is an owner.
         throw new IllegalArgumentException();
      }
      final Object key = new MagicKey(m.getName(), cache(1, "atomic"));

      Map<String, String> map = getAtomicMap(cache, key);
      Map<String, String> map2 = getFineGrainedAtomicMap(cache, key);
   }

   @Override
   protected <CK, K, V> Map<K, V> createAtomicMap(Cache<CK, Object> cache, CK key, boolean createIfAbsent) {
      return getAtomicMap(cache, key, createIfAbsent);
   }
}
