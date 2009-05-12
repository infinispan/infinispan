/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.infinispan.atomic;

import org.infinispan.test.fwk.TestCacheManagerFactory;
import static org.infinispan.atomic.AtomicHashMapTestAssertions.assertIsEmpty;
import static org.infinispan.atomic.AtomicHashMapTestAssertions.assertIsEmptyMap;
import org.infinispan.config.Configuration;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

@Test(groups = "functional", testName = "atomic.APITest")
public class APITest {

   AtomicMapCache cache;
   TransactionManager tm;

   @BeforeTest
   private void setUp() {
      Configuration c = new Configuration();
      c.setInvocationBatchingEnabled(true);
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      cache = (AtomicMapCache) TestCacheManagerFactory.createCacheManager(c).getCache();
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterTest
   private void tearDown() {
      cache.getCacheManager().stop();
   }

   @AfterMethod
   private void clearUp() throws SystemException {
      if (tm.getTransaction() != null) {
         try {
            tm.rollback();
         } catch (Exception ignored) {
            // try to suspend?
            tm.suspend();
         }
      }
      cache.clear();
   }

   public void testAtomicMap() {
      AtomicMap map = cache.getAtomicMap("map");

      assertIsEmpty(map);
      assertIsEmptyMap(cache, "map");

      map.put("blah", "blah");
      assert map.size() == 1;
      assert map.get("blah").equals("blah");
      assert map.containsKey("blah");

      map.clear();

      assertIsEmpty(map);
      assertIsEmptyMap(cache, "map");
   }


   public void testReadSafetyEmptyCache() throws Exception {
      AtomicMap map = cache.getAtomicMap("map");

      assertIsEmpty(map);
      assertIsEmptyMap(cache, "map");

      tm.begin();
      map.put("blah", "blah");
      assert map.size() == 1;
      assert map.get("blah").equals("blah");
      assert map.containsKey("blah");
      Transaction t = tm.suspend();

      assertIsEmpty(map);
      assertIsEmptyMap(cache, "map");

      tm.resume(t);
      tm.commit();

      assert map.size() == 1;
      assert map.get("blah").equals("blah");
      assert map.containsKey("blah");

      map.clear();

      assertIsEmpty(map);
      assertIsEmptyMap(cache, "map");
   }

   public void testReadSafetyNotEmptyCache() throws Exception {
      AtomicMap map = cache.getAtomicMap("map");

      tm.begin();
      map.put("blah", "blah");
      assert map.get("blah").equals("blah");

      Transaction t = tm.suspend();
      assert map.size() == 0;
      assertIsEmpty(map);
      assertIsEmptyMap(cache, "map");

      tm.resume(t);
      tm.commit();

      assert map.size() == 1;
      assert map.get("blah").equals("blah");
      assert map.containsKey("blah");

      map.clear();

      assertIsEmpty(map);
      assertIsEmptyMap(cache, "map");
   }

   public void testReadSafetyRollback() throws Exception {
      AtomicMap map = cache.getAtomicMap("map");

      tm.begin();
      map.put("blah", "blah");
      assert map.size() == 1;
      assert map.get("blah").equals("blah");
      assert map.containsKey("blah");
      Transaction t = tm.suspend();

      assertIsEmpty(map);
      assertIsEmptyMap(cache, "map");

      tm.resume(t);
      tm.rollback();

      assertIsEmpty(map);
      assertIsEmptyMap(cache, "map");
   }
}
