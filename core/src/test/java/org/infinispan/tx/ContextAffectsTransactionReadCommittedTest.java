package org.infinispan.tx;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import jakarta.transaction.Transaction;

/**
 * This test is to ensure that values in the context are properly counted for various cache operations
 *
 * @author wburns
 * @since 6.0
 */
@Test (groups = "functional", testName = "tx.ContextAffectsTransactionReadCommittedTest")
public class ContextAffectsTransactionReadCommittedTest extends SingleCacheManagerTest {

   protected StorageType storage;

   @Factory
   public Object[] factory() {
      return new Object[] {
            new ContextAffectsTransactionReadCommittedTest().withStorage(StorageType.BINARY),
            new ContextAffectsTransactionReadCommittedTest().withStorage(StorageType.HEAP),
            new ContextAffectsTransactionReadCommittedTest().withStorage(StorageType.OFF_HEAP)
      };
   }

   public ContextAffectsTransactionReadCommittedTest withStorage(StorageType storage) {
      this.storage = storage;
      return this;
   }

   @Override
   protected String parameters() {
      return "[storage=" + storage + "]";
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.memory().storage(storage);
      configure(builder);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   protected void configure(ConfigurationBuilder builder) {
      builder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
   }

   public void testSizeAfterClearInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         assertEquals("v1", cache.get(1));

         //clear is non transactional
         cache.clear();

         assertEquals(1, cache.size());
         assertEquals("v1", cache.get(1));
      } finally {
         safeCommit(false);
      }
   }

   public void testEntrySetAfterClearInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         assertEquals("v1", cache.get(1));

         //clear is non transactional
         cache.clear();

         Set<Map.Entry<Object, Object>> entrySet = cache.entrySet();
         assertEquals(1, entrySet.size());
         assertTrue(entrySet.contains(TestingUtil.createMapEntry(1, "v1")));

         Iterator<Map.Entry<Object, Object>> iterator = entrySet.iterator();
         Map.Entry<Object, Object> entry = iterator.next();
         assertEquals(1, entry.getKey());
         assertEquals("v1", entry.getValue());

         assertFalse(iterator.hasNext());
      } finally {
         safeCommit(false);
      }
   }

   public void testKeySetAfterClearInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         assertEquals("v1", cache.get(1));

         //clear is non transactional
         cache.clear();

         Set<Object> keySet = cache.keySet();
         assertEquals(1, keySet.size());
         assertTrue(keySet.contains(1));

         Iterator<Object> iterator = keySet.iterator();
         Object key = iterator.next();
         assertEquals(1, key);

         assertFalse(iterator.hasNext());
      } finally {
         safeCommit(false);
      }
   }

   public void testValuesAfterClearInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
      assertEquals("v1", cache.get(1));

      //clear is non transactional
      cache.clear();

      Collection<Object> values = cache.values();
      assertEquals(1, values.size());
      assertTrue(values.contains("v1"));

      Iterator<Object> iterator = values.iterator();
      Object value = iterator.next();
      assertEquals("v1", value);

      assertFalse(iterator.hasNext());
      } finally {
         safeCommit(false);
      }
   }

   public void testSizeAfterClearInBranchedTransactionOnWrite() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
      assertEquals("v1", cache.put(1, "v2"));

      //clear is non transactional
      cache.clear();

      assertEquals(1, cache.size());
      assertEquals("v2", cache.get(1));
      } finally {
         safeCommit(true);
      }
   }

   public void testEntrySetAfterClearInBranchedTransactionOnWrite() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
      assertEquals("v1", cache.put(1, "v2"));

      //clear is non transactional
      cache.clear();

      Set<Map.Entry<Object, Object>> entrySet = cache.entrySet();
      assertEquals(1, entrySet.size());

      Map.Entry<Object, Object> entry = entrySet.iterator().next();
      assertEquals(1, entry.getKey());
      assertEquals("v2", entry.getValue());
      assertTrue(entrySet.contains(TestingUtil.<Object, Object>createMapEntry(1, "v2")));
      } finally {
         safeCommit(true);
      }
   }

   public void testKeySetAfterClearInBranchedTransactionOnWrite() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
      assertEquals("v1", cache.put(1, "v2"));

      //clear is non transactional
      cache.clear();

      Set<Object> keySet = cache.keySet();
      assertEquals(1, keySet.size());
      assertTrue(keySet.contains(1));
      } finally {
         safeCommit(true);
      }
   }

   public void testValuesAfterClearInBranchedTransactionOnWrite() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
      assertEquals("v1", cache.put(1, "v2"));

      //clear is non transactional
      cache.clear();

      Collection<Object> values = cache.values();
      assertEquals(1, values.size());

      assertTrue(values.contains("v2"));
      } finally {
         safeCommit(true);
      }
   }

   public void testSizeAfterRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      try {
      assertEquals("v1", cache.get(1));

      Transaction suspended = tm().suspend();
      cache.remove(1);
      tm().resume(suspended);

      assertEquals(2, cache.size());

      assertEquals("v1", cache.get(1));
      assertEquals("v2", cache.get(2));
      } finally {
         safeCommit(false);
      }
   }

   public void testEntrySetAfterRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      try {
      assertEquals("v1", cache.get(1));

      Transaction suspended = tm().suspend();
      cache.remove(1);
      tm().resume(suspended);

      Set<Map.Entry<Object, Object>> entrySet = cache.entrySet();
      assertEquals(2, entrySet.size());

      for (Map.Entry<Object, Object> entry : entrySet) {
         Object key = entry.getKey();
         Object value = entry.getValue();
         if (entry.getKey().equals(1)) {
            assertEquals("v1", value);
         } else if (key.equals(2)) {
            assertEquals("v2", value);
         } else {
            fail("Unexpected entry found: " + entry);
         }
      }

      assertTrue(entrySet.contains(TestingUtil.<Object, Object>createMapEntry(1, "v1")));
      assertTrue(entrySet.contains(TestingUtil.<Object, Object>createMapEntry(2, "v2")));
      } finally {
         safeCommit(false);
      }
   }

   public void testKeySetAfterRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      try {
      assertEquals("v1", cache.get(1));

      Transaction suspended = tm().suspend();
      cache.remove(1);
      tm().resume(suspended);

      Set<Object> keySet = cache.keySet();
      assertEquals(2, keySet.size());

      assertTrue(keySet.contains(1));
      assertTrue(keySet.contains(2));
      } finally {
         safeCommit(false);
      }
   }

   public void testValuesAfterRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      try {
      assertEquals("v1", cache.get(1));

      Transaction suspended = tm().suspend();
      cache.remove(1);
      tm().resume(suspended);

      Collection<Object> values = cache.values();
      assertEquals(2, values.size());

      assertTrue(values.contains("v1"));
      assertTrue(values.contains("v2"));
      } finally {
         safeCommit(false);
      }
   }

   public void testSizeAfterDoubleRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      try {
      assertEquals("v1", cache.remove(1));

      Transaction suspended = tm().suspend();
      assertEquals("v1", cache.remove(1));
      tm().resume(suspended);

      assertEquals(1, cache.size());
      assertEquals("v2", cache.get(2));
      } finally {
         safeCommit(true);
      }
   }

   public void testEntrySetAfterDoubleRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      try {
         assertEquals("v1", cache.remove(1));

         Transaction suspended = tm().suspend();
         assertEquals("v1", cache.remove(1));
         tm().resume(suspended);

         Set<Map.Entry<Object, Object>> entrySet = cache.entrySet();
         assertEquals(1, entrySet.size());

         Map.Entry<Object, Object> entry = entrySet.iterator().next();
         assertEquals(2, entry.getKey());
         assertEquals("v2", entry.getValue());
         assertTrue(entrySet.contains(TestingUtil.<Object, Object>createMapEntry(2, "v2")));
      } finally {
         safeCommit(true);
      }
   }

   public void testKeySetAfterDoubleRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      try {
         assertEquals("v1", cache.remove(1));

         Transaction suspended = tm().suspend();
         assertEquals("v1", cache.remove(1));
         tm().resume(suspended);

         Set<Object> keySet = cache.keySet();
         assertEquals(1, keySet.size());
         assertTrue(keySet.contains(2));
      } finally {
         safeCommit(true);
      }
   }

   public void testValuesAfterDoubleRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      try {
         assertEquals("v1", cache.remove(1));

         Transaction suspended = tm().suspend();
         assertEquals("v1", cache.remove(1));
         tm().resume(suspended);

         Collection<Object> values = cache.values();
         assertEquals(1, values.size());
         assertTrue(values.contains("v2"));
      } finally {
         safeCommit(true);
      }
   }

   public void testSizeAfterPutInBranchedTransactionButRemove() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         assertEquals("v1", cache.remove(1));

         Transaction suspended = tm().suspend();
         assertEquals("v1", cache.put(1, "v2"));
         tm().resume(suspended);

         assertEquals(0, cache.size());
      } finally {
         safeCommit(true);
      }
   }

   public void testEntrySetAfterPutInBranchedTransactionButRemove() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         assertEquals("v1", cache.remove(1));

         Transaction suspended = tm().suspend();
         assertEquals("v1", cache.put(1, "v2"));
         tm().resume(suspended);

         Set<Map.Entry<Object, Object>> entrySet = cache.entrySet();
         assertEquals(0, entrySet.size());
         assertFalse(entrySet.iterator().hasNext());
         assertFalse(entrySet.contains(TestingUtil.<Object, Object>createMapEntry(1, "v2")));
      } finally {
         safeCommit(true);
      }
   }

   public void testKeySetAfterPutInBranchedTransactionButRemove() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         assertEquals("v1", cache.remove(1));

         Transaction suspended = tm().suspend();
         assertEquals("v1", cache.put(1, "v2"));
         tm().resume(suspended);

         Set<Object> keySet = cache.keySet();
         assertEquals(0, keySet.size());
         assertFalse(keySet.iterator().hasNext());
         assertFalse(keySet.contains(1));
      } finally {
         safeCommit(true);
      }
   }

   public void testValuesAfterPutInBranchedTransactionButRemove() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         assertEquals("v1", cache.remove(1));

         Transaction suspended = tm().suspend();
         assertEquals("v1", cache.put(1, "v2"));
         tm().resume(suspended);

         Collection<Object> values = cache.values();
         assertEquals(0, values.size());
         assertFalse(values.iterator().hasNext());
         assertFalse(values.contains("v2"));
      } finally {
         safeCommit(true);
      }
   }

   protected void safeCommit(boolean throwWriteSkew) throws Exception {
      tm().commit();
   }
}
