package org.infinispan.api;

import static org.infinispan.test.TestingUtil.v;
import static org.infinispan.test.TestingUtil.withTx;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.transaction.TransactionManager;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Tests the {@link org.infinispan.Cache} public API at a high level
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 */
@Test(groups = "functional")
public abstract class CacheAPITest extends APINonTxTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      ConfigurationBuilder cb = getDefaultStandaloneCacheConfig(true);
      cb.locking().isolationLevel(getIsolationLevel());
      addEviction(cb);
      amend(cb);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      cm.defineConfiguration("test", cb.build());
      cache = cm.getCache("test");
      return cm;
   }

   protected void amend(ConfigurationBuilder cb) {
   }

   protected abstract IsolationLevel getIsolationLevel();

   protected ConfigurationBuilder addEviction(ConfigurationBuilder cb) {
      return cb;
   }

   /**
    * Tests that the configuration contains the values expected, as well as immutability of certain elements
    */
   public void testConfiguration() {
      Configuration c = cache.getCacheConfiguration();
      assertEquals(CacheMode.LOCAL, c.clustering().cacheMode());
      assertNotNull(c.transaction().transactionManagerLookup());
   }

   public void testGetMembersInLocalMode() {
      assertNull("Cache members should be null if running in LOCAL mode", manager(cache).getAddress());
   }

   public void testRollbackAfterOverwrite() throws Exception {
      String key = "key", value = "value", value2 = "value2";
      int size = 1;
      cache.put(key, value);
      assertEquals(value, cache.get(key));
      assertEquals(size, cache.size());
      assertEquals(size, cache.keySet().size());
      assertEquals(size, cache.values().size());
      assertEquals(size, cache.entrySet().size());

      assertTrue(cache.keySet().contains(key));
      assertTrue(cache.values().contains(value));

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      TestingUtil.withTx(tm, () -> {
         assertEquals(value, cache.put(key, value2));
         assertEquals(value2, cache.get(key));
         assertEquals(size, cache.size());
         assertEquals(size, cache.keySet().size());
         assertEquals(size, cache.values().size());
         assertEquals(size, cache.entrySet().size());

         assertTrue(cache.keySet().contains(key));
         assertTrue(cache.values().contains(value2));
         assertFalse(cache.values().contains(value));
         tm.setRollbackOnly();
         return null;
      });

      assertEquals(value, cache.get(key));
      assertEquals(size, cache.size());
      assertEquals(size, cache.keySet().size());
      assertEquals(size, cache.values().size());
      assertEquals(size, cache.entrySet().size());

      assertTrue(cache.keySet().contains(key));
      assertTrue(cache.values().contains(value));
   }

   public void testRollbackAfterRemove() throws Exception {
      String key = "key", value = "value";
      cache.put(key, value);
      assertEquals(value, cache.get(key));
      int size = 1;
      assertEquals(size, cache.size());
      assertEquals(size, cache.keySet().size());
      assertEquals(size, cache.values().size());
      assertEquals(size, cache.entrySet().size());

      assertTrue(cache.keySet().contains(key));
      assertTrue(cache.values().contains(value));

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      withTx(tm, () -> {
         assertEquals(value, cache.remove(key));
         assertNull(value, cache.get(key));

         int tmSize = 0;
         assertEquals(tmSize, cache.size());
         assertEquals(tmSize, cache.keySet().size());
         assertEquals(tmSize, cache.values().size());
         assertEquals(tmSize, cache.entrySet().size());

         assertFalse(cache.keySet().contains(key));
         assertFalse(cache.values().contains(value));
         tm.setRollbackOnly();
         return false;
      });

      assertEquals(value, cache.get(key));
      size = 1;
      assertEquals(size, cache.size());
      assertEquals(size, cache.keySet().size());
      assertEquals(size, cache.values().size());
      assertEquals(size, cache.entrySet().size());

      assertTrue(cache.keySet().contains(key));
      assertTrue(cache.values().contains(value));
   }

   public void testEntrySetEqualityInTx(Method m) throws Exception {
      Map<Object, Object> dataIn = new HashMap<>();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      cache.putAll(dataIn);

      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      withTx(tm, () -> {
         Map<Integer, String> txDataIn = new HashMap<>();
         txDataIn.put(3, v(m, 3));
         Map<Object, Object> allEntriesIn = new HashMap<>(dataIn);

         // Modify expectations to include data to be included
         allEntriesIn.putAll(txDataIn);

         // Add an entry within tx
         cache.putAll(txDataIn);

         Set<Map.Entry<Object, Object>> entries = cache.entrySet();
         assertEquals(allEntriesIn.entrySet(), entries);
         return null;
      });
   }

   public void testEntrySetIterationBeforeInTx(Method m) throws Exception {
      Map<Integer, String> dataIn = new HashMap<>();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      cache.putAll(dataIn);

      Map<Object, Object> foundValues = new HashMap<>();
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      withTx(tm, () -> {
         Set<Entry<Object, Object>> entries = cache.entrySet();

         // Add an entry within tx
         cache.put(3, v(m, 3));
         cache.put(4, v(m, 4));

         for (Entry<Object, Object> entry : entries) {
            foundValues.put(entry.getKey(), entry.getValue());
         }
         tm.setRollbackOnly();
         return null;
      });
      assertEquals(4, foundValues.size());
      assertEquals(v(m, 1), foundValues.get(1));
      assertEquals(v(m, 2), foundValues.get(2));
      assertEquals(v(m, 3), foundValues.get(3));
      assertEquals(v(m, 4), foundValues.get(4));
   }

   public void testEntrySetIterationAfterInTx(Method m) throws Exception {
      Map<Integer, String> dataIn = new HashMap<>();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      cache.putAll(dataIn);

      Map<Object, Object> foundValues = new HashMap<>();
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      withTx(tm, () -> {
         Set<Entry<Object, Object>> entries = cache.entrySet();

         Iterator<Entry<Object, Object>> itr = entries.iterator();

         // Add an entry within tx
         cache.put(3, v(m, 3));
         cache.put(4, v(m, 4));

         while (itr.hasNext()) {
            Entry<Object, Object> entry = itr.next();
            foundValues.put(entry.getKey(), entry.getValue());
         }
         tm.setRollbackOnly();
         return null;
      });
      assertEquals(4, foundValues.size());
      assertEquals(v(m, 1), foundValues.get(1));
      assertEquals(v(m, 2), foundValues.get(2));
      assertEquals(v(m, 3), foundValues.get(3));
      assertEquals(v(m, 4), foundValues.get(4));
   }

   public void testRollbackAfterPut() throws Exception {
      String key = "key", value = "value", key2 = "keyTwo", value2 = "value2";
      cache.put(key, value);
      assertEquals(value, cache.get(key));
      int size = 1;
      assertEquals(size, cache.size());
      assertEquals(size, cache.keySet().size());
      assertEquals(size, cache.values().size());
      assertEquals(size, cache.entrySet().size());
      assertTrue(cache.keySet().contains(key));
      assertTrue(cache.values().contains(value));

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      withTx(tm, () -> {
         cache.put(key2, value2);
         assertEquals(value2, cache.get(key2));
         assertTrue(cache.keySet().contains(key2));
         int tmSize = 2;
         assertEquals(tmSize, cache.size());
         assertEquals(tmSize, cache.keySet().size());
         assertEquals(tmSize, cache.values().size());
         assertEquals(tmSize, cache.entrySet().size());

         assertTrue(cache.values().contains(value2));
         assertTrue(cache.values().contains(value));

         tm.setRollbackOnly();
         return null;
      });

      assertEquals(value, cache.get(key));
      assertEquals(size, cache.size());
      assertEquals(size, cache.keySet().size());
      assertEquals(size, cache.values().size());
      assertEquals(size, cache.entrySet().size());
      assertTrue(cache.keySet().contains(key));
      assertTrue(cache.values().contains(value));
   }

   public void testSizeAfterClear() {
      for (int i = 0; i < 10; i++) {
         cache.put(i, "value" + i);
      }

      cache.clear();

      assertTrue(cache.isEmpty());
   }

   public void testPutIfAbsentAfterRemoveInTx() throws Exception {
      String key = "key_1", old_value = "old_value";
      cache.put(key, old_value);
      assertEquals(old_value, cache.get(key));

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      withTx(tm, () -> {
         assertEquals(old_value, cache.remove(key));
         assertNull(cache.get(key));
         assertEquals(cache.putIfAbsent(key, "new_value"), null);
         tm.setRollbackOnly();
         return null;
      });

      assertEquals(old_value, cache.get(key));
   }

   public void testSizeInExplicitTxWithNonExistent() throws Exception {
      assertEquals(0, cache.size());
      cache.put("k", "v");

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      withTx(tm, () -> {
         assertNull(cache.get("no-exist"));
         assertEquals(1, cache.size());
         assertNull(cache.put("no-exist", "value"));
         assertEquals(2, cache.size());
         tm.setRollbackOnly();
         return null;
      });
   }

   public void testSizeInExplicitTxWithRemoveNonExistent() throws Exception {
      assertEquals(0, cache.size());
      cache.put("k", "v");

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      withTx(tm, () -> {
         assertNull(cache.remove("no-exist"));
         assertEquals(
               1, cache.size());
         assertNull(cache.put("no-exist", "value"));
         assertEquals(2, cache.size());
         tm.setRollbackOnly();
         return null;
      });
   }

   public void testSizeInExplicitTxWithRemoveExistent() throws Exception {
      assertEquals(0, cache.size());
      cache.put("k", "v");

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      withTx(tm, () -> {
         assertNull(cache.put("exist", "value"));
         assertEquals(2, cache.size());
         assertEquals("value", cache.remove("exist"));
         assertEquals(1, cache.size());
         tm.setRollbackOnly();
         return null;
      });
   }

   public void testSizeInExplicitTx() throws Exception {
      assertEquals(0, cache.size());
      cache.put("k", "v");

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      withTx(tm, () -> {
         assertEquals(1, cache.size());
         tm.setRollbackOnly();
         return null;
      });
   }

   public void testSizeInExplicitTxWithModification() throws Exception {
      assertEquals(0, cache.size());
      cache.put("k1", "v1");

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      withTx(tm, () -> {
         assertNull(cache.put("k2", "v2"));
         assertEquals(2, cache.size());
         tm.setRollbackOnly();
         return null;
      });
   }

   public void testEntrySetIteratorRemoveInExplicitTx() throws Exception {
      assertEquals(0, cache.size());
      cache.put("k1", "v1");

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      withTx(tm, () -> {
         try (CloseableIterator<Entry<Object, Object>> entryIterator = cache.entrySet().iterator()) {
            entryIterator.next();
            entryIterator.remove();
            assertEquals(0, cache.size());
         }
         tm.setRollbackOnly();
         return null;
      });
   }

   public void testKeySetIteratorRemoveInExplicitTx() throws Exception {
      assertEquals(0, cache.size());
      cache.put("k1", "v1");

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      withTx(tm, () -> {
         for (CloseableIterator<Object> entryIterator = cache.keySet().iterator(); entryIterator.hasNext(); ) {
            entryIterator.next();
            entryIterator.remove();
            assertEquals(0, cache.size());
         }
         tm.setRollbackOnly();
         return null;
      });
   }

   public void testEntrySetIteratorRemoveContextEntryInExplicitTx() throws Exception {
      assertEquals(0, cache.size());
      cache.put("k1", "v1");

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      withTx(tm, () -> {
         // This should be removed by iterator as well as the k1 entry
         cache.put("k2", "v2");
         assertEquals(2, cache.size());
         for (CloseableIterator<Entry<Object, Object>> entryIterator = cache.entrySet().iterator(); entryIterator.hasNext(); ) {
            entryIterator.next();
            entryIterator.remove();
         }
         assertEquals(0, cache.size());
         tm.setRollbackOnly();
         return null;
      });

      assertEquals(1, cache.size());
   }

   public void testKeySetIteratorRemoveContextEntryInExplicitTx() throws Exception {
      assertEquals(0, cache.size());
      cache.put("k1", "v1");

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      withTx(tm, () -> {
         // This should be removed by iterator as well as the k1 entry
         cache.put("k2", "v2");
         assertEquals(2, cache.size());
         for (CloseableIterator<Object> keyIterator = cache.keySet().iterator(); keyIterator.hasNext(); ) {
            keyIterator.next();
            keyIterator.remove();
         }
         assertEquals(0, cache.size());
         tm.setRollbackOnly();
         return null;
      });

      assertEquals(1, cache.size());
   }

   public void testEntrySetForEachNonSerializable() {
      assertEquals(0, cache.size());
      cache.put("k1", "v1");

      List<Object> values = new ArrayList<>();
      cache.entrySet().forEach(values::add);

      assertEquals(1, values.size());
      Map.Entry<Object, Object> entry = (Map.Entry<Object, Object>) values.iterator().next();
      assertEquals("k1", entry.getKey());
      assertEquals("v1", entry.getValue());
   }

   public void testKeySetForEachNonSerializable() {
      assertEquals(0, cache.size());
      cache.put("k1", "v1");

      List<Object> values = new ArrayList<>();
      cache.keySet().forEach(values::add);

      assertEquals(1, values.size());
      assertEquals("k1", values.iterator().next());
   }

   public void testValuesForEachNonSerializable() {
      assertEquals(0, cache.size());
      cache.put("k1", "v1");

      List<Object> values = new ArrayList<>();
      cache.values().forEach(values::add);

      assertEquals(1, values.size());
      assertEquals("v1", values.iterator().next());
   }
}
