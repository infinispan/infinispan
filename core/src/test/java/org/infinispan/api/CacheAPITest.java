/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.api;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ObjectDuplicator;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertNull;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests the {@link org.infinispan.Cache} public API at a high level
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 */
@Test(groups = "functional")
public abstract class CacheAPITest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      Configuration c = getDefaultStandaloneConfig(true);
      c.setIsolationLevel(getIsolationLevel());
      c = addEviction(c);
      amend(c);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      cm.defineConfiguration("test", c);
      cache = cm.getCache("test");
      return cm;
   }

   protected void amend(Configuration c) {
   }

   protected abstract IsolationLevel getIsolationLevel();

   protected Configuration addEviction(Configuration cfg) {
      return cfg; // No eviction by default
   }

   /**
    * Tests that the configuration contains the values expected, as well as immutability of certain elements
    */
   public void testConfiguration() {
      Configuration c = cache.getConfiguration();
      assert Configuration.CacheMode.LOCAL.equals(c.getCacheMode());
      assert null != c.getTransactionManagerLookupClass();

      // note that certain values should be immutable.  E.g., CacheMode cannot be changed on the fly.
      try {
         c.setCacheMode(Configuration.CacheMode.REPL_SYNC);
         assert false : "Should have thrown an Exception";
      }
      catch (ConfigurationException e) {
         // expected
      }

      // others should be changeable though.
      c.setLockAcquisitionTimeout(100);
   }

   public void testGetMembersInLocalMode() {
      assert manager(cache).getAddress() == null : "Cache members should be null if running in LOCAL mode";
   }

   public void testConvenienceMethods() {
      String key = "key", value = "value";
      Map<String, String> data = new HashMap<String, String>();
      data.put(key, value);

      assert cache.get(key) == null;
      assert cache.keySet().isEmpty();
      assert cache.values().isEmpty();
      assert cache.entrySet().isEmpty();

      cache.put(key, value);

      assert value.equals(cache.get(key));
      assert 1 == cache.keySet().size() && 1 == cache.values().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      cache.remove(key);

      assert cache.get(key) == null;
      assert cache.keySet().isEmpty();
      assert cache.values().isEmpty();
      assert cache.entrySet().isEmpty();

      cache.putAll(data);

      assert value.equals(cache.get(key));
      assert 1 == cache.keySet().size() && 1 == cache.values().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);
   }

   /**
    * Tests basic eviction
    */
   public void testEvict() {
      String key1 = "keyOne", key2 = "keyTwo", value = "value";
      int size = 0;

      cache.put(key1, value);
      cache.put(key2, value);

      assert cache.containsKey(key1);
      assert cache.containsKey(key2);
      size = 2;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key1);
      assert cache.keySet().contains(key2);
      assert cache.values().contains(value);

      // evict two
      cache.evict(key2);

      assert cache.containsKey(key1);
      assert !cache.containsKey(key2);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key1);
      assert !cache.keySet().contains(key2);
      assert cache.values().contains(value);

      cache.evict(key1);

      assert !cache.containsKey(key1);
      assert !cache.containsKey(key2);
      assert cache.isEmpty();
      assert !cache.keySet().contains(key1);
      assert !cache.keySet().contains(key2);
      assert cache.keySet().isEmpty();
      assert !cache.values().contains(value);
      assert cache.values().isEmpty();
      assert cache.entrySet().isEmpty();
   }

   public void testStopClearsData() throws Exception {
      String key = "key", value = "value";
      int size = 0;
      cache.put(key, value);
      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      cache.stop();
      assert cache.getStatus() == ComponentStatus.TERMINATED;
      cache.start();

      assert !cache.containsKey(key);
      assert cache.isEmpty();
      assert !cache.keySet().contains(key);
      assert cache.keySet().isEmpty();
      assert !cache.values().contains(value);
      assert cache.values().isEmpty();
      assert cache.entrySet().isEmpty();
   }

   public void testRollbackAfterPut() throws Exception {
      String key = "key", value = "value", key2 = "keyTwo", value2 = "value2";
      int size = 0;
      cache.put(key, value);
      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      TestingUtil.getTransactionManager(cache).begin();
      cache.put(key2, value2);
      assert cache.get(key2).equals(value2);
      assert cache.keySet().contains(key2);
      size = 2;
      System.out.println(cache.size());
      assert size == cache.size();
      assert size == cache.keySet().size();
      assert size == cache.values().size();
      assert size == cache.entrySet().size();
      assert cache.values().contains(value2);
      TestingUtil.getTransactionManager(cache).rollback();

      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);
   }

   public void testRollbackAfterOverwrite() throws Exception {
      String key = "key", value = "value", value2 = "value2";
      int size = 0;
      cache.put(key, value);
      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      TestingUtil.getTransactionManager(cache).begin();
      cache.put(key, value2);
      assert cache.get(key).equals(value2);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value2);
      TestingUtil.getTransactionManager(cache).rollback();

      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);
   }

   public void testRollbackAfterRemove() throws Exception {
      String key = "key", value = "value";
      int size = 0;
      cache.put(key, value);
      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      TestingUtil.getTransactionManager(cache).begin();
      cache.remove(key);
      assert cache.get(key) == null;
      size = 0;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      TestingUtil.getTransactionManager(cache).rollback();

      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);
   }

   public void testRollbackAfterClear() throws Exception {
      String key = "key", value = "value";
      int size = 0;
      cache.put(key, value);
      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      TestingUtil.getTransactionManager(cache).begin();
      cache.clear();
      assert cache.get(key) == null;
      size = 0;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      TestingUtil.getTransactionManager(cache).rollback();

      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);
   }

   public void testEntrySetEqualityInTx(Method m) throws Exception {
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      cache.putAll(dataIn);

      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      tm.begin();
      try {
         Map txDataIn = new HashMap();
         txDataIn.put(3, v(m, 3));
         Map allEntriesIn = new HashMap(dataIn);

         // Modify expectations to include data to be included
         allEntriesIn.putAll(txDataIn);

         // Add an entry within tx
         cache.putAll(txDataIn);

         Set entries = cache.entrySet();
         assertEquals(allEntriesIn.entrySet(), entries);
      } finally {
         tm.commit();
      }
   }

   public void testConcurrentMapMethods() {

      assert cache.putIfAbsent("A", "B") == null;
      assert cache.putIfAbsent("A", "C").equals("B");
      assert cache.get("A").equals("B");

      assert !cache.remove("A", "C");
      assert cache.containsKey("A");
      assert cache.remove("A", "B");
      assert !cache.containsKey("A");

      cache.put("A", "B");

      assert !cache.replace("A", "D", "C");
      assert cache.get("A").equals("B");
      assert cache.replace("A", "B", "C");
      assert cache.get("A").equals("C");

      assert cache.replace("A", "X").equals("C");
      assert cache.replace("X", "A") == null;
      assert !cache.containsKey("X");
   }

   public void testSizeAndContents() throws Exception {
      String key = "key", value = "value";
      int size = 0;

      assert cache.isEmpty();
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert !cache.containsKey(key);
      assert !cache.keySet().contains(key);
      assert !cache.values().contains(value);

      cache.put(key, value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.containsKey(key);
      assert !cache.isEmpty();
      assert cache.containsKey(key);
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      assert cache.remove(key).equals(value);

      assert cache.isEmpty();
      size = 0;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert !cache.containsKey(key);
      assert !cache.keySet().contains(key);
      assert !cache.values().contains(value);

      Map<String, String> m = new HashMap<String, String>();
      m.put("1", "one");
      m.put("2", "two");
      m.put("3", "three");
      cache.putAll(m);

      assert cache.get("1").equals("one");
      assert cache.get("2").equals("two");
      assert cache.get("3").equals("three");
      size = 3;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();

      m = new HashMap<String, String>();
      m.put("1", "newvalue");
      m.put("4", "four");

      cache.putAll(m);

      assert cache.get("1").equals("newvalue");
      assert cache.get("2").equals("two");
      assert cache.get("3").equals("three");
      assert cache.get("4").equals("four");
      size = 4;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
   }

   public void testKeyValueEntryCollections() {
      String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);
      assert 3 == cache.size() && 3 == cache.keySet().size() && 3 == cache.values().size() && 3 == cache.entrySet().size();

      Set expKeys = new HashSet();
      expKeys.add(key1);
      expKeys.add(key2);
      expKeys.add(key3);

      Set expValues = new HashSet();
      expValues.add(value1);
      expValues.add(value2);
      expValues.add(value3);

      Set expKeyEntries = ObjectDuplicator.duplicateSet(expKeys);
      Set expValueEntries = ObjectDuplicator.duplicateSet(expValues);

      Set<Object> keys = cache.keySet();
      for (Object key : keys) {
         assert expKeys.remove(key);
      }
      assert expKeys.isEmpty() : "Did not see keys " + expKeys + " in iterator!";

      Collection<Object> values = cache.values();
      for (Object value : values) {
         assert expValues.remove(value);
      }
      assert expValues.isEmpty() : "Did not see keys " + expValues + " in iterator!";

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      for (Map.Entry entry : entries) {
         assert expKeyEntries.remove(entry.getKey());
         assert expValueEntries.remove(entry.getValue());
      }
      assert expKeyEntries.isEmpty() : "Did not see keys " + expKeyEntries + " in iterator!";
      assert expValueEntries.isEmpty() : "Did not see keys " + expValueEntries + " in iterator!";
   }

   public void testImmutabilityOfKeyValueEntryCollections() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Set<Object> keys = cache.keySet();
      Collection<Object> values = cache.values();
      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      Collection[] collections = new Collection[]{keys, values, entries};
      Object newObj = new Object();
      List newObjCol = new ArrayList();
      newObjCol.add(newObj);
      for (Collection col : collections) {
         try {
            col.add(newObj);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         } catch (ClassCastException e) {
            // Ignore class cast in expired filtered set because
            // you cannot really add an Object type instance.
         }
         try {
            col.addAll(newObjCol);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }

         try {
            col.clear();
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }

         try {
            col.remove(key1);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }

         try {
            col.removeAll(newObjCol);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }

         try {
            col.retainAll(newObjCol);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
      }

      for (Map.Entry entry : entries) {
         try {
            entry.setValue(newObj);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
      }
   }

   public void testSizeAfterClear() {
      for (int i = 0; i < 10; i++) {
         cache.put(i, "value" + i);
      }

      cache.clear();

      assert cache.isEmpty();
   }

   public void testPutIfAbsentAfterRemoveInTx() throws SystemException, NotSupportedException {
      String key = "key_1", old_value = "old_value", new_value = "new_value";
      cache.put(key, old_value);
      assert cache.get(key).equals(old_value);

      TestingUtil.getTransactionManager(cache).begin();
      assert cache.remove(key).equals(old_value);
      assert cache.get(key) == null;
//      assertEquals(cache.putIfAbsent(key, new_value), null);
      TestingUtil.getTransactionManager(cache).rollback();

      assertEquals(old_value, cache.get(key));
   }
}
