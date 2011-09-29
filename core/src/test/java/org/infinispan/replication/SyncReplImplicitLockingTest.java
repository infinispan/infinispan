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
package org.infinispan.replication;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.assertNoLocks;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Tests for implicit locking
 * <p/>
 * Transparent eager locking for transactions https://jira.jboss.org/jira/browse/ISPN-70
 *
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "replication.SyncReplImplicitLockingTest")
public class SyncReplImplicitLockingTest extends MultipleCacheManagersTest {
   //Cache<String, String> cache1, cache2;
   String k = "key", v = "value";

   public SyncReplImplicitLockingTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected Configuration.CacheMode getCacheMode() {
      return Configuration.CacheMode.REPL_SYNC;
   }

   protected void createCacheManagers() throws Throwable {
      Configuration cfg = getDefaultClusteredConfig(getCacheMode(), true);
      cfg.fluent().transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      cfg.setLockAcquisitionTimeout(500);
      cfg.setUseEagerLocking(true);
      createClusteredCaches(2, "testcache", cfg);
   }

   public void testBasicOperation() throws Exception {
      testBasicOperationHelper(false);
      testBasicOperationHelper(true);
   }

   public void testLocksReleasedWithNoMods() throws Exception {
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);
      Cache cache1 = cache(0,"testcache");
      Cache cache2 = cache(1,"testcache");
      assertNull("Should be null", cache1.get(k));
      assertNull("Should be null", cache2.get(k));

      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();

      // do a dummy read
      cache1.get(k);
      mgr.commit();

      assertNoLocks(cache1);
      assertNoLocks(cache2);
      cache1.clear();cache2.clear();
   }
   
   public void testReplaceNonExistentKey() throws Exception {      
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);
      Cache cache1 = cache(0,"testcache");
      Cache cache2 = cache(1,"testcache");
     
      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();

		// do a replace on empty key
		// https://jira.jboss.org/browse/ISPN-514
		Object old = cache1.replace(k, "blah");

		boolean replaced = cache1.replace(k, "Vladimir", "Blagojevic");
		assert !replaced;

		assertNull("Should be null", cache1.get(k));
		assertNull("Should be null", cache2.get(k));

		mgr.commit();

		assertNoLocks(cache1);
		assertNoLocks(cache2);
		cache1.clear();
		cache2.clear();
	}  

   private void testBasicOperationHelper(boolean useCommit) throws Exception {
      Cache cache1 = cache(0,"testcache");
      Cache cache2 = cache(1,"testcache");
      
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      assertNull("Should be null", cache1.get(k));
      assertNull("Should be null", cache2.get(k));

      String name = "Infinispan";
      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();

      cache1.put(k, name);
      //automatically locked on another cache node
      assertLocked(cache2, k);

      String key2 = "name";
      cache1.put(key2, "Vladimir");
      //automatically locked on another cache node
      assertLocked(cache2, key2);

      String key3 = "product";
      String key4 = "org";
      Map<String, String> newMap = new HashMap<String, String>();
      newMap.put(key3, "Infinispan");
      newMap.put(key4, "JBoss");
      cache1.putAll(newMap);

      //automatically locked on another cache node
      assertLocked(cache2, key3);
      assertLocked(cache2, key4);


      if (useCommit)
         mgr.commit();
      else
         mgr.rollback();

      if (useCommit) {
         assertEquals(name, cache1.get(k));
         assertEquals("Should have replicated", name, cache2.get(k));
      } else {
         assertEquals(null, cache1.get(k));
         assertEquals("Should not have replicated", null, cache2.get(k));
      }

      cache2.remove(k);
      cache2.remove(key2);
      cache2.remove(key3);
      cache2.remove(key4);
   }

   public void testSimpleCommit() throws Throwable{
      tm(0, "testcache").begin();
      cache(0, "testcache").put("k","v");
      tm(0, "testcache").commit();
      assertEquals(cache(0, "testcache").get("k"), "v");
      assertEquals(cache(1, "testcache").get("k"), "v");
      assert !lockManager(0, "testcache").isLocked("k");
      assert !lockManager(1, "testcache").isLocked("k");


      tm(0, "testcache").begin();
      cache(0, "testcache").put("k","v");
      cache(0, "testcache").remove("k");
      tm(0, "testcache").commit();
      assertEquals(cache(0, "testcache").get("k"), null);
      assertEquals(cache(1, "testcache").get("k"), null);

      assert !lockManager(0, "testcache").isLocked("k");
      assert !lockManager(1, "testcache").isLocked("k");
   }

   public void testSimpleRollabck() throws Throwable{
      tm(0, "testcache").begin();
      cache(0, "testcache").put("k","v");
      tm(0, "testcache").rollback();
      assert !lockManager(1, "testcache").isLocked("k");

      assertEquals(cache(0, "testcache").get("k"), null);
      assertEquals(cache(1, "testcache").get("k"), null);
      assert !lockManager(0, "testcache").isLocked("k");
   }
}
