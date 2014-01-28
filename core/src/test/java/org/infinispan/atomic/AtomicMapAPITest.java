package org.infinispan.atomic;

import org.infinispan.Cache;
import org.testng.annotations.Test;

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
   public void testAtomicMapAfterFineGrainedAtomicMap() throws Exception {
      Cache<String, Object> cache1 = cache(0, "atomic");

      Map<String, String> map = getFineGrainedAtomicMap(cache1, "testReplicationRemoveCommit");
      Map<String, String> map2 = getAtomicMap(cache1, "testReplicationRemoveCommit");
   }

   @Override
   protected <CK, K, V> Map<K, V> createAtomicMap(Cache<CK, Object> cache, CK key, boolean createIfAbsent) {
      return getAtomicMap(cache, key, createIfAbsent);
   }
}
