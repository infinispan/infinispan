package org.infinispan.scattered;

import org.infinispan.Cache;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.*;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "scattered.APIScatteredTest")
public class APIScatteredTest extends BaseScatteredTest {
   private static final String VALUE1 = "VALUE1";
   private static final String VALUE2 = "VALUE2";
   private static final String VALUE3 = "VALUE3";

   public void testPut() {
      testPut(false);
   }

   public void testGetAndPut() {
      testPut(true);
   }

   private void testPut(boolean returnValue) {
      final MagicKey KEY = new MagicKey(cache(0));
      assertNull(cacheWithFlags(0, returnValue).put(KEY, VALUE1));
      assertTrue(dc(0).containsKey(KEY));
      assertTrue(dc(1).containsKey(KEY));
      assertFalse(dc(2).containsKey(KEY));
      checkCaches(KEY, VALUE1);

      assertEquals(cacheWithFlags(1, returnValue).put(KEY, VALUE2), returnValue ? VALUE1 : null);
      assertEquals(dc(0).peek(KEY).getValue(), VALUE2);
      assertEquals(dc(1).peek(KEY).getValue(), VALUE2);
      assertFalse(dc(2).containsKey(KEY));
      checkCaches(KEY, VALUE2);

      assertEquals(cacheWithFlags(2, returnValue).put(KEY, VALUE3), returnValue ? VALUE2 : null);
      assertEquals(dc(0).peek(KEY).getValue(), VALUE3);
      assertEquals(dc(2).peek(KEY).getValue(), VALUE3);
      InternalCacheEntry entry1 = dc(1).peek(KEY);
      assertEquals(entry1.getValue(), VALUE2); // the cache should still have the old value
      checkCaches(KEY, VALUE3);
      flush(false);
      assertFalse(dc(1).containsKey(KEY));
   }

   private Cache<Object, Object> cacheWithFlags(int node, boolean returnValue) {
      if (returnValue) {
         return cache(node);
      } else {
         return cache(node).getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);
      }
   }

   private void checkCaches(MagicKey key1, String VALUE1) {
      for (Cache c : caches()) {
         assertEquals(c.get(key1), VALUE1);
      }
   }

   // after remove, there should be a tombstone
   public void testRemove() {
      final MagicKey KEY = new MagicKey(cache(0));
      cache(0).put(KEY, VALUE1);
      EntryVersion oldVersion = dc(0).peek(KEY).getMetadata().version();

      assertEquals(cache(0).remove(KEY), VALUE1);
      InternalCacheEntry entry = dc(0).peek(KEY);
      assertNotNull(entry);
      assertNull(entry.getValue());
      assertNotNull(entry.getMetadata());
      EntryVersion removedVersion = entry.getMetadata().version();
      assertNotNull(removedVersion);
      assertEquals(removedVersion.compareTo(oldVersion), InequalVersionComparisonResult.AFTER);
      checkCaches(KEY, null);

      assertNull(cache(1).remove(KEY));
      entry = dc(0).peek(KEY);
      assertNotNull(entry);
      assertNull(entry.getValue());
      assertNotNull(entry.getMetadata());
      EntryVersion secondRemovedVersion = entry.getMetadata().version();
      assertNotNull(removedVersion);
      assertEquals(secondRemovedVersion.compareTo(removedVersion), InequalVersionComparisonResult.AFTER);

      flush(true);
      for (Cache c : caches()) {
         assertFalse(c.getAdvancedCache().getDataContainer().containsKey(KEY));
      }
   }

   public void testConditionalReplace() {
      final MagicKey KEY = new MagicKey(cache(0));
      cache(0).put(KEY, VALUE1);
      assertTrue(cache(0).replace(KEY, VALUE1, VALUE2));
      assertFalse(cache(0).replace(KEY, VALUE1, VALUE3));
      assertTrue(cache(1).replace(KEY, VALUE2, VALUE3));
      assertFalse(cache(1).replace(KEY, VALUE2, VALUE1));
      assertTrue(cache(2).replace(KEY, VALUE3, VALUE1));
      assertFalse(cache(2).replace(KEY, VALUE3, VALUE2));
   }

   public void testPutAll() {
      Map<Object, Object> data = new HashMap<>();
      final MagicKey KEY1 = new MagicKey(cache(0));
      final MagicKey KEY2 = new MagicKey(cache(1));
      final MagicKey KEY3 = new MagicKey(cache(2));
      data.put(KEY1, VALUE1);
      data.put(KEY2, VALUE2);
      data.put(KEY3, VALUE3);
      cache(0).putAll(data);

      // is either primary or backup for all of them
      assertTrue(dc(0).containsKey(KEY1));
      assertTrue(dc(0).containsKey(KEY2));
      assertTrue(dc(0).containsKey(KEY3));

      assertTrue(dc(1).containsKey(KEY1));
      assertTrue(dc(1).containsKey(KEY2));
      assertFalse(dc(1).containsKey(KEY3));

      assertFalse(dc(2).containsKey(KEY1));
      assertFalse(dc(2).containsKey(KEY2));
      assertTrue(dc(2).containsKey(KEY3));
   }

   public void testGetAll() {
      final MagicKey KEY1 = new MagicKey(cache(0));
      final MagicKey KEY2 = new MagicKey(cache(1));
      final MagicKey KEY3 = new MagicKey(cache(2));
      Set<Object> keys = new HashSet<>(Arrays.asList(KEY1, KEY2, KEY3));
      cache(0).put(KEY1, VALUE1);
      cache(1).put(KEY2, VALUE2);
      cache(2).put(KEY3, VALUE3);

      for (Cache c : caches()) {
         Map<Object, Object> map = c.getAdvancedCache().getAll(keys);
         assertEquals(map.get(KEY1), VALUE1);
         assertEquals(map.get(KEY2), VALUE2);
         assertEquals(map.get(KEY3), VALUE3);
         assertEquals(map.size(), 3);
      }
   }
}
