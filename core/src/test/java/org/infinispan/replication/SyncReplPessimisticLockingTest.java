package org.infinispan.replication;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.jgroups.util.Util.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Tests for implicit locking
 * <p/>
 * Transparent eager locking for transactions https://jira.jboss.org/jira/browse/ISPN-70
 *
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "replication.SyncReplPessimisticLockingTest")
public class SyncReplPessimisticLockingTest extends MultipleCacheManagersTest {

   private String k = "key", v = "value";

   public SyncReplPessimisticLockingTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(getCacheMode(), true);
      cfg.transaction().transactionManagerLookup(new DummyTransactionManagerLookup())
            .lockingMode(LockingMode.PESSIMISTIC)
            .locking().lockAcquisitionTimeout(500);
      createClusteredCaches(2, "testcache", cfg);
   }

   public void testBasicOperation() throws Exception {
      testBasicOperationHelper(false);
      testBasicOperationHelper(true);
   }

   public void testLocksReleasedWithNoMods() throws Exception {
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);
      Cache cache1 = cache(0, "testcache");
      Cache cache2 = cache(1, "testcache");
      assertNull("Should be null", cache1.get(k));
      assertNull("Should be null", cache2.get(k));

      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();

      // do a dummy read
      cache1.get(k);
      mgr.commit();

      assertNotLocked(cache1);
      assertNotLocked(cache2);
      cache1.clear();
      cache2.clear();
   }

   public void testReplaceNonExistentKey() throws Exception {
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);
      Cache cache1 = cache(0, "testcache");
      Cache cache2 = cache(1, "testcache");

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

      assertNotLocked(cache1);
      assertNotLocked(cache2);
      cache1.clear();
      cache2.clear();
   }

   private void testBasicOperationHelper(boolean useCommit) throws Exception {
      Cache cache1 = cache(0, "testcache");
      Cache cache2 = cache(1, "testcache");

      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      assertNull("Should be null", cache1.get(k));
      assertNull("Should be null", cache2.get(k));

      String name = "Infinispan";
      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();

      cache1.put(k, name);
      //automatically locked on another cache node
      assertKeyLockedCorrectly(k, "testcache");

      String key2 = "name";
      cache1.put(key2, "Vladimir");
      //automatically locked on another cache node
      assertKeyLockedCorrectly(key2, "testcache");

      String key3 = "product";
      String key4 = "org";
      Map<String, String> newMap = new HashMap<String, String>();
      newMap.put(key3, "Infinispan");
      newMap.put(key4, "JBoss");
      cache1.putAll(newMap);

      //automatically locked on another cache node
      assertLocked(getLockOwner(key3, "testcache"), key3);
      assertLocked(getLockOwner(key4, "testcache"), key4);


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

   public void testSimpleCommit() throws Throwable {
      tm(0, "testcache").begin();
      cache(0, "testcache").put("k", "v");
      tm(0, "testcache").commit();
      assertEquals(cache(0, "testcache").get("k"), "v");
      assertEquals(cache(1, "testcache").get("k"), "v");

      assertNotLocked("testcache", "k");


      tm(0, "testcache").begin();
      cache(0, "testcache").put("k", "v");
      cache(0, "testcache").remove("k");
      tm(0, "testcache").commit();
      assertEquals(cache(0, "testcache").get("k"), null);
      assertEquals(cache(1, "testcache").get("k"), null);

      assertNotLocked("testcache", "k");
   }

   public void testSimpleRollabck() throws Throwable {
      tm(0, "testcache").begin();
      cache(0, "testcache").put("k", "v");
      tm(0, "testcache").rollback();
      assert !lockManager(1, "testcache").isLocked("k");

      assertEquals(cache(0, "testcache").get("k"), null);
      assertEquals(cache(1, "testcache").get("k"), null);
      assert !lockManager(0, "testcache").isLocked("k");
   }

   @Test
   public void testRemoteLocksReleasedWhenReadTransactionCommitted() throws Exception {
      testRemoteLocksReleased(false, true);
   }

   @Test
   public void testRemoteLocksReleasedWhenReadTransactionRolledBack() throws Exception {
      testRemoteLocksReleased(false, false);
   }

   @Test
   public void testRemoteLocksReleasedWhenWriteTransactionCommitted() throws Exception {
      testRemoteLocksReleased(true, true);
   }

   @Test
   public void testRemoteLocksReleasedWhenWriteTransactionRolledBack() throws Exception {
      testRemoteLocksReleased(true, false);
   }

   private void testRemoteLocksReleased(boolean write, boolean commit) throws Exception {
      final MagicKey key = new MagicKey(cache(0, "testcache"));
      tm(1, "testcache").begin();
      if (write) {
         cache(1, "testcache").put(key, "somevalue");
      } else {
         cache(1, "testcache").getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get(key);
      }

      Collection<LocalTransaction> localTxs = TestingUtil.getTransactionTable(cache(1, "testcache")).getLocalTransactions();
      assertEquals(1, localTxs.size());
      LocalTransaction localTx = localTxs.iterator().next();
      if (write) {
         assertFalse(localTx.isReadOnly());
      } else {
         assertTrue(localTx.isReadOnly());
      }

      final Collection<RemoteTransaction> remoteTxs = TestingUtil.getTransactionTable(cache(0, "testcache")).getRemoteTransactions();
      assertEquals(1, remoteTxs.size());
      RemoteTransaction remoteTx = remoteTxs.iterator().next();
      assertTrue(remoteTx.getLockedKeys().contains(key));
      assertTrue(TestingUtil.extractLockManager(cache(0, "testcache")).isLocked(key));

      if (commit) {
         tm(1, "testcache").commit();
      } else {
         tm(1, "testcache").rollback();
      }

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return remoteTxs.isEmpty();
         }
      });
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !TestingUtil.extractLockManager(cache(0, "testcache")).isLocked(key);
         }
      });
   }
}
