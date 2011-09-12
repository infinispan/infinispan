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
package org.infinispan.expiry;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "expiry.ExpiryTest")
public class ExpiryTest extends AbstractInfinispanTest {

   CacheContainer cm;

   @BeforeMethod
   public void setUp() {
      cm = TestCacheManagerFactory.createLocalCacheManager();
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
   }

   public void testLifespanExpiryInPut() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      long lifespan = 30000;
      cache.put("k", "v", lifespan, MILLISECONDS);

      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry se = dc.get("k");
      assert se.getKey().equals("k");
      assert se.getValue().equals("v");
      assert se.getLifespan() == lifespan;
      assert se.getMaxIdle() == -1;
      assert !se.isExpired();
      assert cache.get("k").equals("v");
      Thread.sleep(lifespan + 100);
      assert se.isExpired();
      assert cache.get("k") == null;
   }

   public void testIdleExpiryInPut() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      long idleTime = 30000;
      cache.put("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS);

      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry se = dc.get("k");
      assert se.getKey().equals("k");
      assert se.getValue().equals("v");
      assert se.getLifespan() == -1;
      assert se.getMaxIdle() == idleTime;
      assert !se.isExpired();
      assert cache.get("k").equals("v");
      Thread.sleep(idleTime + 100);
      assert se.isExpired();
      assert cache.get("k") == null;
   }

   public void testLifespanExpiryInPutAll() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      final long startTime = System.currentTimeMillis();
      final long lifespan = 10000;
      Map<String, String> m = new HashMap();
      m.put("k1", "v");
      m.put("k2", "v");
      cache.putAll(m, lifespan, MILLISECONDS);
      //stop checking 10ms before expiration to prevent races
      while (System.currentTimeMillis() < startTime + lifespan - 10) {
         assert cache.get("k1").equals("v");
         assert cache.get("k2").equals("v");
         Thread.sleep(100);
      }

      //make sure that in the next 30 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 30000) {
         if (cache.get("k1") == null && cache.get("k2") == null) return;
      }
      assert cache.get("k1") == null;
      assert cache.get("k2") == null;
   }

   public void testIdleExpiryInPutAll() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      final long idleTime = 10000;
      Map<String, String> m = new HashMap();
      m.put("k1", "v");
      m.put("k2", "v");
      cache.putAll(m, -1, MILLISECONDS, idleTime, MILLISECONDS);
      assert cache.get("k1").equals("v");
      assert cache.get("k2").equals("v");

      Thread.sleep(idleTime + 100);

      assert cache.get("k1") == null;
      assert cache.get("k2") == null;
   }

   public void testLifespanExpiryInPutIfAbsent() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      final long startTime = System.currentTimeMillis();
      final long lifespan = 10000;
      assert cache.putIfAbsent("k", "v", lifespan, MILLISECONDS) == null;
      long partial = lifespan / 10;
       // Sleep some time within the lifespan boundaries
      Thread.sleep(lifespan - partial);
      assert cache.get("k").equals("v");
      // Sleep some time that guarantees that it'll be over the lifespan
      Thread.sleep(partial * 2);
      assert cache.get("k") == null;

      //make sure that in the next 2 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 2000) {
         if (cache.get("k") == null) break;
         Thread.sleep(50);
      }
      assert cache.get("k") == null;

      cache.put("k", "v");
      assert cache.putIfAbsent("k", "v", lifespan, MILLISECONDS) != null;
   }

   public void testIdleExpiryInPutIfAbsent() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      long idleTime = 30000;
      assert cache.putIfAbsent("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS) == null;
      assert cache.get("k").equals("v");

      Thread.sleep(idleTime + 100);

      assert cache.get("k") == null;

      cache.put("k", "v");
      assert cache.putIfAbsent("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS) != null;
   }

   public void testLifespanExpiryInReplace() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      final long lifespan = 10000;
      assert cache.get("k") == null;
      assert cache.replace("k", "v", lifespan, MILLISECONDS) == null;
      assert cache.get("k") == null;
      cache.put("k", "v-old");
      assert cache.get("k").equals("v-old");
      long startTime = System.currentTimeMillis();
      assert cache.replace("k", "v", lifespan, MILLISECONDS) != null;
      assert cache.get("k").equals("v");
      while (System.currentTimeMillis() < startTime + lifespan - 10) {
         assert cache.get("k").equals("v");
         Thread.sleep(50);
      }

      //make sure that in the next 2 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 2000) {
         if (cache.get("k") == null) break;
         Thread.sleep(50);
      }
      assert cache.get("k") == null;


      startTime = System.currentTimeMillis();
      cache.put("k", "v");
      assert cache.replace("k", "v", "v2", lifespan, MILLISECONDS);
      while (System.currentTimeMillis() < startTime + lifespan) {
         Object val = cache.get("k");
         //only run the assertion if the time condition still stands
         if (System.currentTimeMillis() < startTime + lifespan - 10) {
            assert val.equals("v2");
         }
         Thread.sleep(50);
      }

      //make sure that in the next 2 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 2000) {
         if (cache.get("k") == null) break;
         Thread.sleep(50);
      }
      assert cache.get("k") == null;
   }

   public void testIdleExpiryInReplace() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      long idleTime = 30000;
      assert cache.get("k") == null;
      assert cache.replace("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS) == null;
      assert cache.get("k") == null;
      cache.put("k", "v-old");
      assert cache.get("k").equals("v-old");
      assert cache.replace("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS) != null;
      assertEquals(cache.get("k"), "v");

      Thread.sleep(idleTime + 100);
      assert cache.get("k") == null;

      cache.put("k", "v");
      assert cache.replace("k", "v", "v2", -1, MILLISECONDS, idleTime, MILLISECONDS);

      Thread.sleep(idleTime + 100);
      assert cache.get("k") == null;
   }

   public void testEntrySetAfterExpiryInPut(Method m) throws Exception {
      doTestEntrySetAfterExpiryInPut(m, cm);
   }

   public void testEntrySetAfterExpiryInTransaction(Method m) throws Exception {
      CacheContainer cc = createTransactionalCacheContainer();
      try {
         doEntrySetAfterExpiryInTransaction(m, cc);
      } finally {
         cc.stop();
      }
   }

   private CacheContainer createTransactionalCacheContainer() {
      return TestCacheManagerFactory.createCacheManager(new Configuration());
   }

   private void doTestEntrySetAfterExpiryInPut(Method m, CacheContainer cc) throws Exception {
      Cache<Integer, String> cache = cc.getCache();
      Set<Map.Entry<Integer, String>> entries;
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));
      Set entriesIn = dataIn.entrySet();

      final long startTime = System.currentTimeMillis();
      final long lifespan = 5000;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      entries = Collections.emptySet();
      // Stop checking 10ms before expiration to prevent races
      while (System.currentTimeMillis() < startTime + lifespan - 10) {
         entries = cache.entrySet();
         assertEquals(entriesIn, entries);
         Thread.sleep(100);
      }

      // Make sure that in the next 20 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 20000) {
         entries = cache.entrySet();
         if (entries.size() == 0) return;
      }

      assert entries.size() == 0;
   }

   private void doEntrySetAfterExpiryInTransaction(Method m, CacheContainer cc) throws Exception {
      Cache<Integer, String> cache = cc.getCache();
      Set<Map.Entry<Integer, String>> entries;
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      final long startTime = System.currentTimeMillis();
      final long lifespan = 5000;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      cache.getAdvancedCache().getTransactionManager().begin();
      try {
         Map txDataIn = new HashMap();
         txDataIn.put(3, v(m, 3));
         Map allEntriesIn = new HashMap(dataIn);
         // Update expectations
         allEntriesIn.putAll(txDataIn);
         // Add an entry within tx
         cache.putAll(txDataIn);

         entries = Collections.emptySet();
         // Stop checking 10ms before expiration to prevent races
         while (System.currentTimeMillis() < startTime + lifespan - 10) {
            entries = cache.entrySet();
            assertEquals(allEntriesIn.entrySet(), entries);
            Thread.sleep(100);
         }

         // Make sure that in the next 20 secs data is removed
         while (System.currentTimeMillis() < startTime + lifespan + 20000) {
            entries = cache.entrySet();
            if (entries.size() == 1) return;
         }
      } finally {
         cache.getAdvancedCache().getTransactionManager().commit();
      }

      assert entries.size() == 1;
   }

   public void testKeySetAfterExpiryInPut(Method m) throws Exception {
      Cache<Integer, String> cache = cm.getCache();
      Set<Integer> keys;
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));
      Set keysIn = dataIn.keySet();

      final long startTime = System.currentTimeMillis();
      final long lifespan = 5000;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      keys = Collections.emptySet();
      // Stop checking 10ms before expiration to prevent races
      while (System.currentTimeMillis() < startTime + lifespan - 10) {
         keys = cache.keySet();
         assertEquals(keysIn, keys);
         Thread.sleep(100);
      }

      // Make sure that in the next 20 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 20000) {
         keys = cache.keySet();
         if (keys.size() == 0) return;
      }

      assert keys.size() == 0;
   }

   public void testKeySetAfterExpiryInTransaction(Method m) throws Exception {
      CacheContainer cc = createTransactionalCacheContainer();
      try {
         doKeySetAfterExpiryInTransaction(m, cc);
      } finally {
         cc.stop();
      }
   }

   private void doKeySetAfterExpiryInTransaction(Method m, CacheContainer cc) throws Exception {
      Cache<Integer, String> cache = cc.getCache();
      Set<Integer> keys;
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      final long startTime = System.currentTimeMillis();
      final long lifespan = 5000;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      cache.getAdvancedCache().getTransactionManager().begin();
      try {
         Map txDataIn = new HashMap();
         txDataIn.put(3, v(m, 3));
         Map allEntriesIn = new HashMap(dataIn);
         // Update expectations
         allEntriesIn.putAll(txDataIn);
         // Add an entry within tx
         cache.putAll(txDataIn);

         keys = Collections.emptySet();
         // Stop checking 10ms before expiration to prevent races
         while (System.currentTimeMillis() < startTime + lifespan - 10) {
            keys = cache.keySet();
            assertEquals(allEntriesIn.keySet(), keys);
            Thread.sleep(100);
         }

         // Make sure that in the next 20 secs data is removed
         while (System.currentTimeMillis() < startTime + lifespan + 20000) {
            keys = cache.keySet();
            if (keys.size() == 1) return;
         }
      } finally {
         cache.getAdvancedCache().getTransactionManager().commit();
      }

      assert keys.size() == 1;
   }

   // TODO: Test values

   public void testValuesAfterExpiryInPut(Method m) throws Exception {
      Cache<Integer, String> cache = cm.getCache();
      // Values come as a Collection, but comparison of HashMap#Values is done
      // by reference equality, so wrap the collection around to set to make
      // testing easier, given that we know that there are dup values.
      Set<String> values;
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));
      Set valuesIn = new HashSet(dataIn.values());

      final long startTime = System.currentTimeMillis();
      final long lifespan = 5000;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      values = Collections.emptySet();
      // Stop checking 10ms before expiration to prevent races
      while (System.currentTimeMillis() < startTime + lifespan - 10) {
         values = new HashSet(cache.values());
         assertEquals(valuesIn, values);
         Thread.sleep(100);
      }

      // Make sure that in the next 20 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 20000) {
         values = new HashSet(cache.values());
         if (values.size() == 0) return;
      }

      assert values.size() == 0;
   }

   public void testValuesAfterExpiryInTransaction(Method m) throws Exception {
      CacheContainer cc = createTransactionalCacheContainer();
      try {
         doValuesAfterExpiryInTransaction(m, cc);
      } finally {
         cc.stop();
      }
   }

   private void doValuesAfterExpiryInTransaction(Method m, CacheContainer cc) throws Exception {
      Cache<Integer, String> cache = cc.getCache();
      // Values come as a Collection, but comparison of HashMap#Values is done
      // by reference equality, so wrap the collection around to set to make
      // testing easier, given that we know that there are dup values.
      Set<String> values;
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      final long startTime = System.currentTimeMillis();
      final long lifespan = 5000;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      cache.getAdvancedCache().getTransactionManager().begin();
      try {
         Map txDataIn = new HashMap();
         txDataIn.put(3, v(m, 3));
         Set allValuesIn = new HashSet(dataIn.values());
         allValuesIn.addAll(txDataIn.values());

         // Add an entry within tx
         cache.putAll(txDataIn);

         values = Collections.emptySet();
         // Stop checking 10ms before expiration to prevent races
         while (System.currentTimeMillis() < startTime + lifespan - 10) {
            values = new HashSet(cache.values());
            assertEquals(allValuesIn, values);
            Thread.sleep(100);
         }

         // Make sure that in the next 20 secs data is removed
         while (System.currentTimeMillis() < startTime + lifespan + 20000) {
            values = new HashSet(cache.values());
            if (values.size() == 1) return;
         }
      } finally {
         cache.getAdvancedCache().getTransactionManager().commit();
      }

      assert values.size() == 1;
   }
}
