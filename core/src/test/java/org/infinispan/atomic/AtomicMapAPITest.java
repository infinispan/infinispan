package org.infinispan.atomic;

import static org.infinispan.atomic.AtomicMapLookup.getAtomicMap;
import static org.infinispan.atomic.AtomicMapLookup.getFineGrainedAtomicMap;
import static org.testng.AssertJUnit.fail;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.distribution.MagicKey;
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

      try {
         getAtomicMap(cache1, key);
         fail("Should have failed with an IllegalArgumentException");
      } catch (IllegalArgumentException e) {
         // Expected
      }
   }

   @Test(enabled = false, description = "Doesn't work when the originator isn't the primary owner, see ISPN-5988")
   public void testAtomicMapAfterFineGrainedAtomicMapBackup() throws Exception {
      Cache<MagicKey, Object> cache1 = cache(0, "atomic");
      Cache<MagicKey, Object> cache2 = cache(1, "atomic");

      MagicKey key = new MagicKey(cache2);
      getFineGrainedAtomicMap(cache1, key);

      try {
         getAtomicMap(cache1, key);
         fail("Should have failed with an IllegalArgumentException");
      } catch (IllegalArgumentException e) {
         // Expected
      }
   }

   @Override
   protected <CK, K, V> Map<K, V> createAtomicMap(Cache<CK, Object> cache, CK key, boolean createIfAbsent) {
      return getAtomicMap(cache, key, createIfAbsent);
   }
}
