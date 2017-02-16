package org.infinispan.atomic;

import static org.infinispan.atomic.AtomicMapLookup.getAtomicMap;
import static org.infinispan.atomic.AtomicMapLookup.getFineGrainedAtomicMap;
import static org.testng.AssertJUnit.fail;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.distribution.MagicKey;
import org.testng.annotations.Test;

/**
 * @author Vladimir Blagojevic (C) 2011 Red Hat Inc.
 * @author Sanne Grinovero (C) 2011 Red Hat Inc.
 * @author Pedro Ruivo
 */
@Test(groups = "functional", testName = "atomic.FineGrainedAtomicMapAPITest")
public class FineGrainedAtomicMapAPITest extends BaseAtomicHashMapAPITest {

   public void testFineGrainedMapAfterAtomicMapPrimary() throws Exception {
      Cache<MagicKey, Object> cache1 = cache(0, "atomic");

      MagicKey key = new MagicKey("key", cache1);
      getAtomicMap(cache1, key);

      try {
         getFineGrainedAtomicMap(cache1, key);
         fail("Should have failed with an IllegalArgumentException");
      } catch (IllegalArgumentException e) {
         // Expected
      }
   }

   @Test(enabled = false, description = "Doesn't work when the originator isn't the primary owner, see ISPN-5988")
   public void testFineGrainedMapAfterAtomicMapBackup() throws Exception {
      Cache<MagicKey, Object> cache1 = cache(0, "atomic");
      Cache<MagicKey, Object> cache2 = cache(1, "atomic");

      MagicKey key = new MagicKey("key", cache2);
      getAtomicMap(cache1, key);

      try {
         getFineGrainedAtomicMap(cache1, key);
         fail("Should have failed with an IllegalArgumentException");
      } catch (IllegalArgumentException e) {
         // Expected
      }
   }

   @Override
   protected <CK, K, V> Map<K, V> createAtomicMap(Cache<CK, Object> cache, CK key, boolean createIfAbsent) {
      return getFineGrainedAtomicMap(cache, key, createIfAbsent);
   }
}
