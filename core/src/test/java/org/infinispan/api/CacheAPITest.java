package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

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
      assert CacheMode.LOCAL.equals(c.clustering().cacheMode());
      assert null != c.transaction().transactionManagerLookup();
   }

   public void testGetMembersInLocalMode() {
      assert manager(cache).getAddress() == null : "Cache members should be null if running in LOCAL mode";
   }

   public void testRollbackAfterOverwrite() throws Exception {
      String key = "key", value = "value", value2 = "value2";
      int size;
      cache.put(key, value);
      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      TestingUtil.getTransactionManager(cache).begin();
      try {
         cache.put(key, value2);
         assert cache.get(key).equals(value2);
         size = 1;
         assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
         assert cache.keySet().contains(key);
         assert cache.values().contains(value2);
      } finally {
         TestingUtil.getTransactionManager(cache).rollback();
      }

      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);
   }

   public void testRollbackAfterRemove() throws Exception {
      String key = "key", value = "value";
      int size;
      cache.put(key, value);
      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      TestingUtil.getTransactionManager(cache).begin();
      try {
         cache.remove(key);
         assert cache.get(key) == null;
         size = 0;
         assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      } finally {
         TestingUtil.getTransactionManager(cache).rollback();
      }

      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);
   }

   public void testEntrySetEqualityInTx(Method m) throws Exception {
      Map<Object, Object> dataIn = new HashMap<>();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      cache.putAll(dataIn);

      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      tm.begin();
      try {
         Map<Integer, String> txDataIn = new HashMap<>();
         txDataIn.put(3, v(m, 3));
         Map<Object, Object> allEntriesIn = new HashMap<>(dataIn);

         // Modify expectations to include data to be included
         allEntriesIn.putAll(txDataIn);

         // Add an entry within tx
         cache.putAll(txDataIn);

         Set<Map.Entry<Object, Object>> entries = cache.entrySet();
         assertEquals(allEntriesIn.entrySet(), entries);
      } finally {
         tm.rollback();
      }
   }

   public void testEntrySetIterationBeforeInTx(Method m) throws Exception {
      Map<Integer, String> dataIn = new HashMap<>();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      cache.putAll(dataIn);

      Map<Object, Object> foundValues = new HashMap<>();
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      tm.begin();
      try {
         Set<Entry<Object, Object>> entries = cache.entrySet();

         // Add an entry within tx
         cache.put(3, v(m, 3));
         cache.put(4, v(m, 4));

         for (Entry<Object, Object> entry : entries) {
            foundValues.put(entry.getKey(), entry.getValue());
         }
      } finally {
         tm.rollback();
      }
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
      tm.begin();
      try {
         Set<Entry<Object, Object>> entries = cache.entrySet();

         Iterator<Entry<Object, Object>> itr = entries.iterator();

         // Add an entry within tx
         cache.put(3, v(m, 3));
         cache.put(4, v(m, 4));

         while (itr.hasNext()) {
            Entry<Object, Object> entry = itr.next();
            foundValues.put(entry.getKey(), entry.getValue());
         }
      } finally {
         tm.rollback();
      }
      assertEquals(4, foundValues.size());
      assertEquals(v(m, 1), foundValues.get(1));
      assertEquals(v(m, 2), foundValues.get(2));
      assertEquals(v(m, 3), foundValues.get(3));
      assertEquals(v(m, 4), foundValues.get(4));
   }

   public void testRollbackAfterPut() throws Exception {
      String key = "key", value = "value", key2 = "keyTwo", value2 = "value2";
      int size;
      cache.put(key, value);
      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      TestingUtil.getTransactionManager(cache).begin();
      try {
         cache.put(key2, value2);
         assert cache.get(key2).equals(value2);
         assert cache.keySet().contains(key2);
         size = 2;
         log.trace(cache.size());
         assert size == cache.size();
         assert size == cache.keySet().size();
         assert size == cache.values().size();
         assert size == cache.entrySet().size();
         assert cache.values().contains(value2);
      } finally {
         TestingUtil.getTransactionManager(cache).rollback();
      }

      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);
   }

   public void testSizeAfterClear() {
      for (int i = 0; i < 10; i++) {
         cache.put(i, "value" + i);
      }

      cache.clear();

      assert cache.isEmpty();
   }

   public void testPutIfAbsentAfterRemoveInTx() throws SystemException, NotSupportedException {
      String key = "key_1", old_value = "old_value";
      cache.put(key, old_value);
      assert cache.get(key).equals(old_value);

      TestingUtil.getTransactionManager(cache).begin();
      try {
         assert cache.remove(key).equals(old_value);
         assert cache.get(key) == null;
   //      assertEquals(cache.putIfAbsent(key, new_value), null);
      } finally {
         TestingUtil.getTransactionManager(cache).rollback();
      }

      assertEquals(old_value, cache.get(key));
   }
}
