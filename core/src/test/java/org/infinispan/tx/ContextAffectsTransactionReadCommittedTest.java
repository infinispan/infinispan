package org.infinispan.tx;

import static org.jgroups.util.Util.assertFalse;
import static org.jgroups.util.Util.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.transaction.Transaction;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * This test is to ensure that values in the context are properly counted for various cache operations
 *
 * @author wburns
 * @since 6.0
 */
@Test (groups = "functional", testName = "tx.ContextAffectsTransactionReadCommittedTest")
public class ContextAffectsTransactionReadCommittedTest extends SingleCacheManagerTest {
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      configure(builder);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   protected void configure(ConfigurationBuilder builder) {
      builder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
   }

   public void testSizeAfterClearInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      assertEquals("v1", cache.get(1));

      //clear is non transactional
      cache.clear();

      assertEquals(1, cache.size());
      assertEquals("v1", cache.get(1));
      safeCommit(false);
   }

   public void testEntrySetAfterClearInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      assertEquals("v1", cache.get(1));

      //clear is non transactional
      cache.clear();

      Set<Map.Entry<Object, Object>> entrySet = cache.entrySet();
      assertEquals(1, entrySet.size());

      Map.Entry<Object, Object> entry = entrySet.iterator().next();
      assertEquals(1, entry.getKey());
      assertEquals("v1", entry.getValue());
      assertTrue(entrySet.contains(TestingUtil.<Object, Object>createMapEntry(1, "v1")));
      safeCommit(false);
   }

   public void testKeySetAfterClearInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      assertEquals("v1", cache.get(1));

      //clear is non transactional
      cache.clear();

      Set<Object> keySet = cache.keySet();
      assertEquals(1, keySet.size());
      assertTrue(keySet.contains(1));
      safeCommit(false);
   }

   public void testValuesAfterClearInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      assertEquals("v1", cache.get(1));

      //clear is non transactional
      cache.clear();

      Collection<Object> values = cache.values();
      assertEquals(1, values.size());
      assertTrue(values.contains("v1"));
      safeCommit(false);
   }

   public void testSizeAfterClearInBranchedTransactionOnWrite() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      assertEquals("v1", cache.put(1, "v2"));

      //clear is non transactional
      cache.clear();

      assertEquals(1, cache.size());
      assertEquals("v2", cache.get(1));
      safeCommit(true);
   }

   public void testEntrySetAfterClearInBranchedTransactionOnWrite() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      assertEquals("v1", cache.put(1, "v2"));

      //clear is non transactional
      cache.clear();

      Set<Map.Entry<Object, Object>> entrySet = cache.entrySet();
      assertEquals(1, entrySet.size());

      Map.Entry<Object, Object> entry = entrySet.iterator().next();
      assertEquals(1, entry.getKey());
      assertEquals("v2", entry.getValue());
      assertTrue(entrySet.contains(TestingUtil.<Object, Object>createMapEntry(1, "v2")));
      safeCommit(true);
   }

   public void testKeySetAfterClearInBranchedTransactionOnWrite() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      assertEquals("v1", cache.put(1, "v2"));

      //clear is non transactional
      cache.clear();

      Set<Object> keySet = cache.keySet();
      assertEquals(1, keySet.size());
      assertTrue(keySet.contains(1));
      safeCommit(true);
   }

   public void testValuesAfterClearInBranchedTransactionOnWrite() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      assertEquals("v1", cache.put(1, "v2"));

      //clear is non transactional
      cache.clear();

      Collection<Object> values = cache.values();
      assertEquals(1, values.size());

      assertTrue(values.contains("v2"));
      safeCommit(true);
   }

   public void testSizeAfterRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      assertEquals("v1", cache.get(1));

      Transaction suspended = tm().suspend();
      cache.remove(1);
      tm().resume(suspended);

      assertEquals(2, cache.size());

      assertEquals("v1", cache.get(1));
      assertEquals("v2", cache.get(2));
      safeCommit(false);
   }

   public void testEntrySetAfterRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
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
      safeCommit(false);
   }

   public void testKeySetAfterRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      assertEquals("v1", cache.get(1));

      Transaction suspended = tm().suspend();
      cache.remove(1);
      tm().resume(suspended);

      Set<Object> keySet = cache.keySet();
      assertEquals(2, keySet.size());

      assertTrue(keySet.contains(1));
      assertTrue(keySet.contains(2));
      safeCommit(false);
   }

   public void testValuesAfterRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      assertEquals("v1", cache.get(1));

      Transaction suspended = tm().suspend();
      cache.remove(1);
      tm().resume(suspended);

      Collection<Object> values = cache.values();
      assertEquals(2, values.size());

      assertTrue(values.contains("v1"));
      assertTrue(values.contains("v2"));
      safeCommit(false);
   }

   public void testSizeAfterDoubleRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      assertEquals("v1", cache.remove(1));

      Transaction suspended = tm().suspend();
      assertEquals("v1", cache.remove(1));
      tm().resume(suspended);

      assertEquals(1, cache.size());
      assertEquals("v2", cache.get(2));
      safeCommit(true);
   }

   public void testEntrySetAfterDoubleRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
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
      safeCommit(true);
   }

   public void testKeySetAfterDoubleRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      assertEquals("v1", cache.remove(1));

      Transaction suspended = tm().suspend();
      assertEquals("v1", cache.remove(1));
      tm().resume(suspended);

      Set<Object> keySet = cache.keySet();
      assertEquals(1, keySet.size());
      assertTrue(keySet.contains(2));
      safeCommit(true);
   }

   public void testValuesAfterDoubleRemoveInBranchedTransaction() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v2");
      tm().begin();
      assertEquals("v1", cache.remove(1));

      Transaction suspended = tm().suspend();
      assertEquals("v1", cache.remove(1));
      tm().resume(suspended);

      Collection<Object> values = cache.values();
      assertEquals(1, values.size());
      assertTrue(values.contains("v2"));
      safeCommit(true);
   }

   public void testSizeAfterPutInBranchedTransactionButRemove() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      assertEquals("v1", cache.remove(1));

      Transaction suspended = tm().suspend();
      assertEquals("v1", cache.put(1, "v2"));
      tm().resume(suspended);

      assertEquals(0, cache.size());
      safeCommit(true);
   }

   public void testEntrySetAfterPutInBranchedTransactionButRemove() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      assertEquals("v1", cache.remove(1));

      Transaction suspended = tm().suspend();
      assertEquals("v1", cache.put(1, "v2"));
      tm().resume(suspended);

      Set<Map.Entry<Object, Object>> entrySet = cache.entrySet();
      assertEquals(0, entrySet.size());
      assertFalse(entrySet.iterator().hasNext());
      assertFalse(entrySet.contains(TestingUtil.<Object, Object>createMapEntry(1, "v2")));
      safeCommit(true);
   }

   public void testKeySetAfterPutInBranchedTransactionButRemove() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      assertEquals("v1", cache.remove(1));

      Transaction suspended = tm().suspend();
      assertEquals("v1", cache.put(1, "v2"));
      tm().resume(suspended);

      Set<Object> keySet = cache.keySet();
      assertEquals(0, keySet.size());
      assertFalse(keySet.iterator().hasNext());
      assertFalse(keySet.contains(1));
      safeCommit(true);
   }

   public void testValuesAfterPutInBranchedTransactionButRemove() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      assertEquals("v1", cache.remove(1));

      Transaction suspended = tm().suspend();
      assertEquals("v1", cache.put(1, "v2"));
      tm().resume(suspended);

      Collection<Object> values = cache.values();
      assertEquals(0, values.size());
      assertFalse(values.iterator().hasNext());
      assertFalse(values.contains("v2"));
      safeCommit(true);
   }

   protected void safeCommit(boolean throwWriteSkew) throws Exception {
      tm().commit();
   }
}
