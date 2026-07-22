package org.infinispan.api;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.RemoveAllTest")
public class RemoveAllTest extends SingleCacheManagerTest {

   private Cache<String, String> c;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      cm.start();
      c = cm.getCache();
      return cm;
   }

   @BeforeMethod
   public void populateCache() {
      c.clear();
      for (int i = 0; i < 10; i++) {
         c.put("key" + i, "value" + i);
      }
   }

   public void testRemoveAll() {
      Set<String> keys = new HashSet<>();
      for (int i = 0; i < 10; i++) {
         keys.add("key" + i);
      }
      c.removeAll(keys);
      for (int i = 0; i < 10; i++) {
         assertNull("key" + i + " should have been removed", c.get("key" + i));
      }
      assertEquals(0, c.size());
   }

   public void testRemoveAllAsync() throws Exception {
      Set<String> keys = new HashSet<>();
      for (int i = 0; i < 10; i++) {
         keys.add("key" + i);
      }
      CompletableFuture<Void> f = c.removeAllAsync(keys);
      assertNotNull(f);
      f.get();
      assertTrue(f.isDone());
      for (int i = 0; i < 10; i++) {
         assertNull(c.get("key" + i));
      }
      assertEquals(0, c.size());
   }

   public void testRemoveAllNonExistent() {
      int sizeBefore = c.size();
      Set<String> keys = Set.of("nonexistent1", "nonexistent2", "nonexistent3");
      c.removeAll(keys);
      assertEquals(sizeBefore, c.size());
      for (int i = 0; i < 10; i++) {
         assertEquals("value" + i, c.get("key" + i));
      }
   }

   public void testRemoveAllPartial() {
      Set<String> keysToRemove = new HashSet<>();
      for (int i = 0; i < 5; i++) {
         keysToRemove.add("key" + i);
      }
      c.removeAll(keysToRemove);
      for (int i = 0; i < 5; i++) {
         assertNull("key" + i + " should have been removed", c.get("key" + i));
      }
      for (int i = 5; i < 10; i++) {
         assertEquals("value" + i, c.get("key" + i));
      }
      assertEquals(5, c.size());
   }

   public void testRemoveAllEmpty() {
      c.removeAll(Collections.emptySet());
      assertEquals(10, c.size());
   }
}
