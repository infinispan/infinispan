package org.infinispan.replication;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.TransactionalEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

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
            // TODO: Another case of default values changed (see ISPN-2651)
            .transaction().useSynchronization(false);

      createClusteredCaches(2, "cache", builder);

      cache1 = cache(0, "cache");
      cache2 = cache(1, "cache");
   }

   public void testSyncTxRepl() throws Exception {
      Integer age;
      TransactionManager tm = TestingUtil.getTransactionManager(cache1);

      tm.begin();
      Transaction tx = tm.getTransaction();
      LocalListener lis = new LocalListener();
      cache1.addListener(lis);
      try {
         lis.put("age", 38);
      } finally {
         cache1.removeListener(lis);
      }

      tm.suspend();
      assertNull("age on cache2 must be null as the TX has not yet been committed", cache2.get("age"));
      tm.resume(tx);
      tm.commit();

      // value on cache2 must be 38
      age = (Integer) cache2.get("age");
      assertNotNull("\"age\" obtained from cache2 must be non-null ", age);
      assertEquals("\"age\" must be 38", (int) age, 38);
   }

   public void testRemoteCacheListener() throws Exception {
      Integer age;
      RemoteListener lis = new RemoteListener();
      cache2.addListener(lis);
      try {
         cache1.put("age", 38);

         // value on cache2 must be 38
         age = (Integer) cache2.get("age");
         assertNotNull("\"age\" obtained from cache2 must be non-null ", age);
         assertEquals("\"age\" must be 38", (int) age, 38);
         cache1.remove("age");
      } finally {
         cache2.removeListener(lis);
      }
   }

   public void testSyncRepl() throws Exception {
      Integer age;
      LocalListener lis = new LocalListener();
      cache1.addListener(lis);
      try {
         lis.put("age", 38);
      } finally {
         cache2.removeListener(lis);
      }

      // value on cache2 must be 38
      age = (Integer) cache2.get("age");
      assertNotNull("\"age\" obtained from cache2 must be non-null ", age);
      assertEquals("\"age\" must be 38", (int) age, 38);
   }


   public void simpleReplicationTest() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache1);
      tm.begin();
      cache1.put("key", "value");
      tm.commit();

      assertEquals("value", cache2.get("key"));

   }

   public void testSyncTxReplMap() throws Exception {
      Integer age;
      TransactionManager tm = TestingUtil.getTransactionManager(cache1);
      tm.begin();
      Transaction tx = tm.getTransaction();
      LocalListener lis = new LocalListener();
      try {
         cache1.put("age", 38);
         lis.put("name", "Ben");
      } finally {
         cache1.removeListener(lis);
      }

      assertEquals(38, cache1.get("age"));
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
      assertEquals("\"age\" must be 38", (int) age, 38);
   }

   public void testSyncReplMap() throws Exception {
      Integer age;
      LockManager lm1 = TestingUtil.extractComponent(cache1, LockManager.class);

      assertNull("lock info is " + lm1.printLockInfo(), lm1.getOwner("age"));
      LocalListener lis = new LocalListener();
      cache1.addListener(lis);
      try {
         lis.put("age", 38);

         cache1.put("name", "Ben");
      } finally {
         cache1.removeListener(lis);
      }

      assertNull("lock info is " + lm1.printLockInfo(), lm1.getOwner("age"));
      // value on cache2 must be 38
      age = (Integer) cache2.get("age");
      assertNotNull("\"age\" obtained from cache2 must be non-null ", age);
      assertEquals("\"age\" must be 38", (int) age, 38);
      assertNull("lock info is " + lm1.printLockInfo(), lm1.getOwner("age"));
   }

   @Listener
   public class LocalListener {
      Object key = null;

      public void put(Object key, Object val) {
         this.key = key;
         cache1.put(key, val);
      }

      public void put(Map<?, ?> map) {
         if (map.isEmpty()) fail("put(): map size can't be 0");
         cache1.putAll(map);
      }

      @CacheEntryModified
      public void modified(Event<Object, Object> ne) {
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
   public static class RemoteListener {

      @CacheEntryRemoved
      @CacheEntryModified
      public void callback(TransactionalEvent e) {
         log.trace("Callback got event " + e);
         log.debug("Callback got event " + e);
         assertFalse("entry was removed on remote cache so isLocal should be false", e.isOriginLocal());
      }
   }
}
