package org.infinispan.atomic;

import static org.infinispan.atomic.AtomicMapLookup.getAtomicMap;
import static org.infinispan.atomic.AtomicMapLookup.getFineGrainedAtomicMap;
import static org.infinispan.test.Exceptions.expectException;
import static org.testng.AssertJUnit.fail;

import java.util.Map;

import javax.transaction.TransactionManager;

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

      expectException(IllegalStateException.class, () -> getFineGrainedAtomicMap(cache1, key));
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

   // Since fine grained map now holds each subentry as separate entry we need to adjust the expected sizes
   @Override
   public void testSizeOnCache() throws Exception {
      final Cache<Object, Object> cache1 = cache(0, "atomic");
      final TransactionManager tm1 = tm(0, "atomic");
      assertSize(cache1, 0);
      cache1.put(new MagicKey("Hi", cache1), "Someone");
      assertSize(cache1, 1);

      tm1.begin();
      assertSize(cache1, 1);
      cache1.put(new MagicKey("Need", cache1), "Read Consistency");
      assertSize(cache1, 2);
      tm1.commit();
      assertSize(cache1, 2);

      tm1.begin();
      assertSize(cache1, 2);
      cache1.put(new MagicKey("Need Also", cache1), "Speed");
      assertSize(cache1, 3);
      tm1.rollback();
      assertSize(cache1, 2);

      Map<Object, Object> atomicMap = createAtomicMap(cache1, new MagicKey("testSizeOnCache", cache1), true);
      assertSize(cache1, 3);
      atomicMap.put("mm", "nn");
      assertSize(cache1, 4);

      tm1.begin();
      assertSize(cache1, 4);
      atomicMap = createAtomicMap(cache1, new MagicKey("testSizeOnCache-second", cache1), true);
      assertSize(cache1, 5);
      atomicMap.put("mm", "nn");
      assertSize(cache1, 6);
      tm1.commit();
      assertSize(cache1, 6);

      tm1.begin();
      assertSize(cache1, 6);
      atomicMap = createAtomicMap(cache1, new MagicKey("testSizeOnCache-third", cache1), true);
      assertSize(cache1, 7);
      atomicMap.put("mm", "nn");
      assertSize(cache1, 8);
      atomicMap.put("ooo", "weird!");
      assertSize(cache1, 9);
      atomicMap = createAtomicMap(cache1, new MagicKey("testSizeOnCache-onemore", cache1), true);
      assertSize(cache1, 10);
      atomicMap.put("even less?", "weird!");
      assertSize(cache1, 11);
      tm1.rollback();
      assertSize(cache1, 6);
   }
}
