package org.infinispan.invalidation;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.List;
import javax.transaction.RollbackException;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.api.mvcc.LockAssert;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commons.tx.TransactionImpl;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

@Test(groups = "functional")
public abstract class BaseInvalidationTest extends MultipleCacheManagersTest {
   boolean isSync;

   protected BaseInvalidationTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(isSync ? CacheMode.INVALIDATION_SYNC : CacheMode.INVALIDATION_ASYNC, false);
      c.clustering().stateTransfer().timeout(10000)
       .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      createClusteredCaches(2, "invalidation", c);

      if (isSync) {
         c = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, true);
         c.clustering().stateTransfer().timeout(10000)
          .transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup())
          .transaction().lockingMode(LockingMode.OPTIMISTIC)
          .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
         defineConfigurationOnAllManagers("invalidationTx", c);

         waitForClusterToForm("invalidationTx");
      }
   }

   public void testRemove() throws Exception {
      AdvancedCache<String, String> cache1 = advancedCache(0,"invalidation");
      AdvancedCache<String, String> cache2 = advancedCache(1,"invalidation");
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      assertEquals("value", cache1.get("key"));
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      assertEquals("value", cache2.get("key"));

      replListener(cache2).expectAny();
      assertEquals("value", cache1.remove("key"));
      replListener(cache2).waitForRpc();

      assertEquals(false, cache2.containsKey("key"));
   }

   public void testResurrectEntry() throws Exception {
      AdvancedCache<String, String> cache1 = advancedCache(0,"invalidation");
      AdvancedCache<String, String> cache2 = advancedCache(1,"invalidation");
      replListener(cache2).expect(InvalidateCommand.class);
      cache1.put("key", "value");
      replListener(cache2).waitForRpc();

      assertEquals("value", cache1.get("key"));
      assertEquals(null, cache2.get("key"));
      replListener(cache2).expect(InvalidateCommand.class);
      cache1.put("key", "newValue");
      replListener(cache2).waitForRpc();

      assertEquals("newValue", cache1.get("key"));
      assertEquals(null, cache2.get("key"));

      replListener(cache2).expect(InvalidateCommand.class);
      assertEquals("newValue", cache1.remove("key"));
      replListener(cache2).waitForRpc();

      assertEquals(null, cache1.get("key"));
      assertEquals(null, cache2.get("key"));

      // Restore locally
      replListener(cache2).expect(InvalidateCommand.class);
      cache1.put("key", "value");
      replListener(cache2).waitForRpc();

      assertEquals("value", cache1.get("key"));
      assertEquals(null, cache2.get("key"));

      replListener(cache1).expect(InvalidateCommand.class);
      cache2.put("key", "value2");
      replListener(cache1).waitForRpc();

      assertEquals("value2", cache2.get("key"));
      assertEquals(null, cache1.get("key"));
   }

   public void testDeleteNonExistentEntry() throws Exception {
      if (!isSync) {
         return;
      }
      AdvancedCache<String, String> cache1 = advancedCache(0,"invalidationTx");
      AdvancedCache<String, String> cache2 = advancedCache(1,"invalidationTx");

      assertNull("Should be null", cache1.get("key"));
      assertNull("Should be null", cache2.get("key"));

      replListener(cache2).expect(InvalidateCommand.class);
      cache1.put("key", "value");
      replListener(cache2).waitForRpc();

      assertEquals("value", cache1.get("key"));
      assertNull("Should be null", cache2.get("key"));

      // OK, here's the real test
      TransactionManager tm = TestingUtil.getTransactionManager(cache2);
      tm.begin();

      // Remove an entry that doesn't exist in cache2
      cache2.remove("key");

      replListener(cache1).expect(InvalidateCommand.class); // invalidates always happen outside of a tx
      tm.commit();
      replListener(cache1).waitForRpc();

      assertNull(cache1.get("key"));
      assertNull(cache2.get("key"));
   }

   public void testTxSyncUnableToInvalidate() throws Exception {
      if (!isSync) {
         return;
      }
      AdvancedCache<String, String> cache1 = advancedCache(0,"invalidationTx");
      AdvancedCache<String, String> cache2 = advancedCache(1,"invalidationTx");
      TransactionManager mgr1 = TestingUtil.getTransactionManager(cache1);
      TransactionManager mgr2 = TestingUtil.getTransactionManager(cache2);
      LockManager lm1 = TestingUtil.extractComponent(cache1, LockManager.class);
      LockManager lm2 = TestingUtil.extractComponent(cache2, LockManager.class);

      replListener(cache2).expect(InvalidateCommand.class);
      cache1.put("key", "value");
      replListener(cache2).waitForRpc();

      assertEquals("value", cache1.get("key"));
      assertNull(cache2.get("key"));

      TransactionImpl tx1 = null;
      mgr1.begin();
      try {
         cache1.put("key", "value2");
         tx1 = (TransactionImpl) mgr1.suspend();

         // Acquire the key lock for tx1 and hold it
         replListener(cache2).expect(InvalidateCommand.class);
         tx1.runPrepare();
         replListener(cache2).waitForRpc();

         mgr2.begin();
         cache2.put("key", "value3");

         // tx2 prepare fails because tx1 is holding the key lock
         replListener(cache1).expect(InvalidateCommand.class);
         Exceptions.expectException(RollbackException.class, mgr2::commit);
         replListener(cache2).assertNoRpc();
      } finally {
         if (tx1 != null) {
            tx1.runCommit(false);
         }
      }

      eventually(() -> !lm1.isLocked("key"));
      eventually(() -> !lm2.isLocked("key"));

      LockAssert.assertNoLocks(cache1);
      LockAssert.assertNoLocks(cache2);
   }

   public void testCacheMode() throws Exception {
      AdvancedCache<String, String> cache1 = advancedCache(0,"invalidation");
      RpcManagerImpl rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache1, RpcManager.class);
      Transport origTransport = TestingUtil.extractComponent(cache1, Transport.class);
      try {
         Transport mockTransport = mock(Transport.class);
         rpcManager.setTransport(mockTransport);
         Address addressOne = mock(Address.class);
         Address addressTwo = mock(Address.class);
         List<Address> members = new ArrayList<>(2);
         members.add(addressOne);
         members.add(addressTwo);

         when(mockTransport.getMembers()).thenReturn(members);
         when(mockTransport.getAddress()).thenReturn(addressOne);
         when(mockTransport.invokeCommandOnAll(any(), any(), any(), any(), anyLong(), any()))
               .thenReturn(CompletableFutures.completedNull());

         cache1.put("k", "v");

      } finally {
         if (rpcManager != null) rpcManager.setTransport(origTransport);
      }
   }

   public void testPutIfAbsent() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"invalidation");
      AdvancedCache<String, String> cache2 = advancedCache(1,"invalidation");
      String putPrevious = cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      assertNull(putPrevious);
      assertEquals("value", cache2.get("key"));
      assertNull(cache1.get("key"));

      replListener(cache2).expect(InvalidateCommand.class);
      String putIfAbsentPrevious = cache1.putIfAbsent("key", "value");
      assertNull(putIfAbsentPrevious);
      replListener(cache2).waitForRpc();

      assertEquals("value", cache1.get("key"));
      String value = cache2.get("key");
      assertNull(value);

      assertNull(cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2"));

      assertEquals("value", cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      cache1.putIfAbsent("key", "value3");

      assertEquals("value", cache1.get("key"));
      assertEquals("value2", cache2.get("key")); // should not invalidate cache2!!
   }

   public void testRemoveIfPresent() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"invalidation");
      AdvancedCache<String, String> cache2 = advancedCache(1,"invalidation");
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assertEquals("value1", cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      assertFalse(cache1.remove("key", "value"));

      assertEquals("Should not remove", "value1", cache1.get("key"));
      assertEquals("Should not evict", "value2", cache2.get("key"));

      replListener(cache2).expect(InvalidateCommand.class);
      cache1.remove("key", "value1");
      replListener(cache2).waitForRpc();

      assertNull(cache1.get("key"));
      assertNull(cache2.get("key"));
   }

   public void testClear() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"invalidation");
      AdvancedCache<String, String> cache2 = advancedCache(1,"invalidation");
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assertEquals("value1", cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      replListener(cache2).expect(ClearCommand.class);
      cache1.clear();
      replListener(cache2).waitForRpc();

      assertNull(cache1.get("key"));
      assertNull(cache2.get("key"));
   }

   public void testReplace() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"invalidation");
      AdvancedCache<String, String> cache2 = advancedCache(1,"invalidation");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assertNull(cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      assertNull(cache1.replace("key", "value1")); // should do nothing since there is nothing to replace on cache1

      assertNull(cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      assertNull(cache1.withFlags(CACHE_MODE_LOCAL).put("key", "valueN"));

      replListener(cache2).expect(InvalidateCommand.class);
      cache1.replace("key", "value1");
      replListener(cache2).waitForRpc();

      assertEquals("value1", cache1.get("key"));
      assertNull(cache2.get("key"));
   }

   public void testReplaceWithOldVal() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"invalidation");
      AdvancedCache<String, String> cache2 = advancedCache(1,"invalidation");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assertNull(cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      assertFalse(
         cache1.replace("key", "valueOld", "value1")); // should do nothing since there is nothing to replace on cache1

      assertNull(cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      assertNull(cache1.withFlags(CACHE_MODE_LOCAL).put("key", "valueN"));

      assertFalse(
         cache1.replace("key", "valueOld", "value1")); // should do nothing since there is nothing to replace on cache1

      assertEquals("valueN", cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      replListener(cache2).expect(InvalidateCommand.class);
      assertTrue(cache1.replace("key", "valueN", "value1"));
      replListener(cache2).waitForRpc();

      assertEquals("value1", cache1.get("key"));
      assertNull(cache2.get("key"));
   }

   public void testLocalOnlyClear() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"invalidation");
      AdvancedCache<String, String> cache2 = advancedCache(1,"invalidation");
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assertEquals("value1", cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      cache1.withFlags(CACHE_MODE_LOCAL).clear();

      assertNull(cache1.get("key"));
      assertNotNull(cache2.get("key"));
      assertEquals("value2", cache2.get("key"));
   }

   public void testPutForExternalRead() throws Exception {
      AdvancedCache<String, String> cache1 = advancedCache(0,"invalidation");
      AdvancedCache<String, String> cache2 = advancedCache(1,"invalidation");
      cache1.putForExternalRead("key", "value1");
      Thread.sleep(500); // sleep so that async invalidation (result of PFER) is propagated
      cache2.putForExternalRead("key", "value2");
      Thread.sleep(500); // sleep so that async invalidation (result of PFER) is propagated
      assertNotNull(cache1.get("key"));
      assertEquals("value1", cache1.get("key"));
      assertNotNull(cache2.get("key"));
      assertEquals("value2", cache2.get("key"));
   }
}
