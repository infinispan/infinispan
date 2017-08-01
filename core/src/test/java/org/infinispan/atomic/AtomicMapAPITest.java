package org.infinispan.atomic;

import static org.infinispan.atomic.AtomicMapLookup.getAtomicMap;
import static org.infinispan.atomic.AtomicMapLookup.getFineGrainedAtomicMap;
import static org.infinispan.test.Exceptions.expectException;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.Exceptions;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "atomic.AtomicMapAPITest")
public class AtomicMapAPITest extends BaseAtomicHashMapAPITest {

   public void testAtomicMapAfterFineGrainedAtomicMapPrimary() throws Exception {
      Cache<MagicKey, Object> cache1 = cache(0, "atomic");

      MagicKey key = new MagicKey(cache1);
      getFineGrainedAtomicMap(cache1, key);

      expectException(IllegalStateException.class, () -> getAtomicMap(cache1, key));
   }

   public void testAtomicMapAfterFineGrainedAtomicMapBackup() throws Exception {
      Cache<MagicKey, Object> cache1 = cache(0, "atomic");
      Cache<MagicKey, Object> cache2 = cache(1, "atomic");

      MagicKey key = new MagicKey(cache2);
      getFineGrainedAtomicMap(cache1, key);

      Exceptions.expectException(IllegalStateException.class, "ISPN000457:.*", () -> getAtomicMap(cache1, key));
   }

   @Override
   protected <CK, K, V> Map<K, V> createAtomicMap(Cache<CK, Object> cache, CK key, boolean createIfAbsent) {
      return getAtomicMap(cache, key, createIfAbsent);
   }
}
