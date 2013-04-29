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

/*
 *
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.infinispan.replication;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.TransactionalEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import static org.testng.AssertJUnit.*;

import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.List;
import java.util.Map;

/**
 * Test out the CacheListener
 */
@Test(groups = "functional", testName = "replication.SyncCacheListenerTest")
public class SyncCacheListenerTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(SyncCacheListenerTest.class);

   private Cache<Object, Object> cache1, cache2;

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder =
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      builder.locking().isolationLevel(IsolationLevel.SERIALIZABLE)
            .lockAcquisitionTimeout(5000)
            // TODO: Another case of default values changed (see ISPN-2651)
            .transaction().useSynchronization(false);

      List<Cache<Object, Object>> caches =
            createClusteredCaches(2, "cache", builder);

      cache1 = caches.get(0);
      cache2 = caches.get(1);
   }
  
   public void testSyncTxRepl() throws Exception {
      Integer age;
      TransactionManager tm = TestingUtil.getTransactionManager(cache1);

      tm.begin();
      Transaction tx = tm.getTransaction();
      LocalListener lis = new LocalListener();
      cache1.addListener(lis);
      lis.put("age", 38);

      tm.suspend();
      assertNull("age on cache2 must be null as the TX has not yet been committed", cache2.get("age"));
      tm.resume(tx);
      tm.commit();

      // value on cache2 must be 38
      age = (Integer) cache2.get("age");
      assertNotNull("\"age\" obtained from cache2 must be non-null ", age);
      assertTrue("\"age\" must be 38", age == 38);
   }

   public void testRemoteCacheListener() throws Exception {
      Integer age;
      RemoteListener lis = new RemoteListener();
      cache2.addListener(lis);
      cache1.put("age", 38);

      // value on cache2 must be 38
      age = (Integer) cache2.get("age");
      assertNotNull("\"age\" obtained from cache2 must be non-null ", age);
      assertTrue("\"age\" must be 38", age == 38);
      cache1.remove("age");
   }

   public void testSyncRepl() throws Exception {
      Integer age;
      LocalListener lis = new LocalListener();
      cache1.addListener(lis);
      lis.put("age", 38);

      // value on cache2 must be 38
      age = (Integer) cache2.get("age");
      assertNotNull("\"age\" obtained from cache2 must be non-null ", age);
      assertTrue("\"age\" must be 38", age == 38);
   }


   public void simpleReplicationTest() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache1);
      tm.begin();
      cache1.put("key", "value");
      tm.commit();

      assert cache2.get("key").equals("value");

   }

   public void testSyncTxReplMap() throws Exception {
      Integer age;
      TransactionManager tm = TestingUtil.getTransactionManager(cache1);
      tm.begin();
      Transaction tx = tm.getTransaction();
      LocalListener lis = new LocalListener();

      cache1.put("age", 38);
      lis.put("name", "Ben");

      assert cache1.get("age").equals(38);
      tm.suspend();
      assertNull("age on cache2 must be null as the TX has not yet been committed", cache2.get("age"));
      assertNull("age on cache1 must be null as the TX has been resumed", cache1.get("age"));
      tm.resume(tx);
      assertNotNull("age on cache1 must be not be null", cache1.get("age"));
      tm.commit();
      assertNotNull("age on cache1 must be not be null", cache1.get("age"));

      log.trace("  ********************** ");
      // value on cache2 must be 38
      age = (Integer) cache2.get("age");
      assertNotNull("\"age\" obtained from cache2 must be non-null ", age);
      assertTrue("\"age\" must be 38", age == 38);
   }

   public void testSyncReplMap() throws Exception {
      Integer age;
      LockManager lm1 = TestingUtil.extractComponent(cache1, LockManager.class);

      assert lm1.getOwner("age") == null : "lock info is " + lm1.printLockInfo();
      LocalListener lis = new LocalListener();
      cache1.addListener(lis);
      lis.put("age", 38);
      assert lm1.getOwner("age") == null : "lock info is " + lm1.printLockInfo();

      cache1.put("name", "Ben");
      // value on cache2 must be 38
      age = (Integer) cache2.get("age");
      assertNotNull("\"age\" obtained from cache2 must be non-null ", age);
      assertTrue("\"age\" must be 38", age == 38);
      assert lm1.getOwner("age") == null : "lock info is " + lm1.printLockInfo();
   }

   @Listener
   public class LocalListener {
      Object key = null;

      public void put(Object key, Object val) {
         this.key = key;
         cache1.put(key, val);
      }

      public void put(Map map) {
         if (map.isEmpty()) fail("put(): map size can't be 0");
         cache1.putAll(map);
      }

      @CacheEntryModified
      public void modified(Event ne) {
         if (!ne.isPre()) {
            log.debug("modified visited with key: " + key);
            try {
               // test out if we can get the read lock since there is a write lock going as well.
               cache1.get(key);
            }
            catch (CacheException e) {
               log.error("Error reading the cache", e);
               throw e;
            }
         }
      }

   }

   @Listener
   static public class RemoteListener {

      @CacheEntryRemoved
      @CacheEntryModified
      public void callback(TransactionalEvent e) {
         log.trace("Callback got event " + e);
         log.debug("Callback got event " + e);
         assertFalse("entry was removed on remote cache so isLocal should be false", e.isOriginLocal());
      }
   }
}
