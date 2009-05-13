package org.infinispan.api.mvcc;

import org.easymock.EasyMock;
import static org.easymock.EasyMock.*;
import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.xa.TransactionTable;
import static org.testng.AssertJUnit.*;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.List;

@Test(groups = "functional", testName = "api.mvcc.PutForExternalReadTest")
public class PutForExternalReadTest extends MultipleCacheManagersTest {
   final String key = "k", value = "v", value2 = "v2";
   Cache cache1, cache2;
   TransactionManager tm1, tm2;
   ReplListener replListener1, replListener2;

   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      createClusteredCaches(2, "replSync", c);

      cache1 = cache(0, "replSync");
      cache2 = cache(1, "replSync");

      tm1 = TestingUtil.getTransactionManager(cache1);
      tm2 = TestingUtil.getTransactionManager(cache2);

      replListener1 = replListener(cache1);
      replListener2 = replListener(cache2);
   }

   public void testNoOpWhenKeyPresent() {
      replListener2.expect(PutKeyValueCommand.class);
      cache1.putForExternalRead(key, value);
      replListener2.waitForRpc();


      assertEquals("PFER should have succeeded", value, cache1.get(key));
      assertEquals("PFER should have replicated", value, cache2.get(key));

      // reset
      replListener2.expect(RemoveCommand.class);
      cache1.remove(key);
      replListener2.waitForRpc();

      assert cache1.isEmpty() : "Should have reset";
      assert cache2.isEmpty() : "Should have reset";

      replListener2.expect(PutKeyValueCommand.class);
      cache1.put(key, value);
      replListener2.waitForRpc();

      // now this pfer should be a no-op
      cache1.putForExternalRead(key, value2);

      assertEquals("PFER should have been a no-op", value, cache1.get(key));
      assertEquals("PFER should have been a no-op", value, cache2.get(key));
   }

   private List<Address> anyAddresses() {
      anyObject();
      return null;
   }

   private ResponseMode anyResponseMode() {
      anyObject();
      return null;
   }

   public void testAsyncForce() throws Exception {
      Transport mockTransport = createNiceMock(Transport.class);
      RpcManagerImpl rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache1, RpcManager.class);
      Transport originalTransport = TestingUtil.extractComponent(cache1, Transport.class);
      try {

         Address mockAddress1 = createNiceMock(Address.class);
         Address mockAddress2 = createNiceMock(Address.class);

         List<Address> memberList = new ArrayList<Address>(2);
         memberList.add(mockAddress1);
         memberList.add(mockAddress2);

         expect(mockTransport.getMembers()).andReturn(memberList).anyTimes();
         rpcManager.setTransport(mockTransport);
         // specify what we expectWithTx called on the mock Rpc Manager.  For params we don't care about, just use ANYTHING.
         // setting the mock object to expectWithTx the "sync" param to be false.
         expect(mockTransport.invokeRemotely((List<Address>) anyObject(), (CacheRpcCommand) anyObject(),
                                             eq(ResponseMode.ASYNCHRONOUS), anyLong(), anyBoolean(), (ResponseFilter) isNull(), anyBoolean())).andReturn(null);

         replay(mockAddress1, mockAddress2, mockTransport);

         // now try a simple replication.  Since the RpcManager is a mock object it will not actually replicate anything.
         cache1.putForExternalRead(key, value);
         verify(mockTransport);

      } finally {
         if (rpcManager != null) rpcManager.setTransport(originalTransport);
      }
   }

   public void testTxSuspension() throws Exception {
      replListener2.expect(PutKeyValueCommand.class);
      cache1.put(key + "0", value);
      replListener2.waitForRpc();

      // start a tx and do some stuff.
      replListener2.expect(PutKeyValueCommand.class);
      tm1.begin();
      cache1.get(key + "0");
      cache1.putForExternalRead(key, value); // should have happened in a separate tx and have committed already.
      Transaction t = tm1.suspend();

      replListener2.waitForRpc();
      assertEquals("PFER should have completed", value, cache1.get(key));
      assertEquals("PFER should have completed", value, cache2.get(key));

      tm1.resume(t);
      tm1.commit();

      assertEquals("tx should have completed", value, cache1.get(key + "0"));
      assertEquals("tx should have completed", value, cache2.get(key + "0"));
   }

   public void testExceptionSuppression() throws Exception {
      Transport mockTransport = createNiceMock(Transport.class);
      RpcManagerImpl rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache1, RpcManager.class);
      Transport originalTransport = TestingUtil.extractComponent(cache1, Transport.class);
      try {

         Address mockAddress1 = createNiceMock(Address.class);
         Address mockAddress2 = createNiceMock(Address.class);

         List<Address> memberList = new ArrayList<Address>(2);
         memberList.add(mockAddress1);
         memberList.add(mockAddress2);

         expect(mockTransport.getMembers()).andReturn(memberList).anyTimes();
         rpcManager.setTransport(mockTransport);


         expect(mockTransport.invokeRemotely(anyAddresses(), (CacheRpcCommand) anyObject(), anyResponseMode(),
                                             anyLong(), anyBoolean(), (ResponseFilter) anyObject(), anyBoolean()))
               .andThrow(new RuntimeException("Barf!")).anyTimes();

         replay(mockTransport);

         try {
            cache1.put(key, value);
            fail("Should have barfed");
         }
         catch (RuntimeException re) {
         }

         // clean up any indeterminate state left over
         try {
            cache1.remove(key);
            fail("Should have barfed");
         }
         catch (RuntimeException re) {
         }

         assertNull("Should have cleaned up", cache1.get(key));

         // should not barf
         cache1.putForExternalRead(key, value);
      }
      finally {
         if (rpcManager != null) rpcManager.setTransport(originalTransport);
      }
   }

   public void testBasicPropagation() throws Exception {
      assert !cache1.containsKey(key);
      assert !cache2.containsKey(key);

      replListener2.expect(PutKeyValueCommand.class);
      cache1.putForExternalRead(key, value);
      replListener2.waitForRpc();

      assertEquals("PFER updated cache1", value, cache1.get(key));
      assertEquals("PFER propagated to cache2 as expected", value, cache2.get(key));

      // replication to cache 1 should NOT happen.
      cache2.putForExternalRead(key, value + "0");

      assertEquals("PFER updated cache2", value, cache2.get(key));
      assertEquals("Cache1 should be unaffected", value, cache1.get(key));
   }

   /**
    * Tests that setting a cacheModeLocal=true flag prevents propagation of the putForExternalRead().
    *
    * @throws Exception
    */
   public void testSimpleCacheModeLocal() throws Exception {
      cacheModeLocalTest(false);
   }

   /**
    * Tests that setting a cacheModeLocal=true flag prevents propagation of the putForExternalRead() when the call
    * occurs inside a transaction.
    *
    * @throws Exception
    */
   public void testCacheModeLocalInTx() throws Exception {
      cacheModeLocalTest(true);
   }

   /**
    * Tests that suspended transactions do not leak.  See JBCACHE-1246.
    */
   public void testMemLeakOnSuspendedTransactions() throws Exception {
      replListener2.expect(PutKeyValueCommand.class);
      tm1.begin();
      cache1.putForExternalRead(key, value);
      tm1.commit();
      replListener2.waitForRpc();

      TransactionTable tt1 = TestingUtil.extractComponent(cache1, TransactionTable.class);
      TransactionTable tt2 = TestingUtil.extractComponent(cache2, TransactionTable.class);

      assert tt1.getRemoteTxCount() == 0 : "Cache 1 should have no stale global TXs";
      assert tt1.getLocalTxCount() == 0 : "Cache 1 should have no stale local TXs";
      assert tt2.getRemoteTxCount() == 0 : "Cache 2 should have no stale global TXs";
      assert tt2.getLocalTxCount() == 0 : "Cache 2 should have no stale local TXs";

      System.out.println("PutForExternalReadTest.testMemLeakOnSuspendedTransactions");
      replListener2.expectWithTx(PutKeyValueCommand.class);
      tm1.begin();
      cache1.putForExternalRead(key, value);
      cache1.put(key, value);
      tm1.commit();
      replListener2.waitForRpc();

      assert tt1.getRemoteTxCount() == 0 : "Cache 1 should have no stale global TXs";
      assert tt1.getLocalTxCount() == 0 : "Cache 1 should have no stale local TXs";
      assert tt2.getRemoteTxCount() == 0 : "Cache 2 should have no stale global TXs";
      assert tt2.getLocalTxCount() == 0 : "Cache 2 should have no stale local TXs";

      replListener2.expectWithTx(PutKeyValueCommand.class);
      tm1.begin();
      cache1.put(key, value);
      cache1.putForExternalRead(key, value);
      tm1.commit();
      replListener2.waitForRpc();

      assert tt1.getRemoteTxCount() == 0 : "Cache 1 should have no stale global TXs";
      assert tt1.getLocalTxCount() == 0 : "Cache 1 should have no stale local TXs";
      assert tt2.getRemoteTxCount() == 0 : "Cache 2 should have no stale global TXs";
      assert tt2.getLocalTxCount() == 0 : "Cache 2 should have no stale local TXs";

      replListener2.expectWithTx(PutKeyValueCommand.class, PutKeyValueCommand.class);
      tm1.begin();
      cache1.put(key, value);
      cache1.putForExternalRead(key, value);
      cache1.put(key, value);
      tm1.commit();
      replListener2.waitForRpc();

      assert tt1.getRemoteTxCount() == 0 : "Cache 1 should have no stale global TXs";
      assert tt1.getLocalTxCount() == 0 : "Cache 1 should have no stale local TXs";
      assert tt2.getRemoteTxCount() == 0 : "Cache 2 should have no stale global TXs";
      assert tt2.getLocalTxCount() == 0 : "Cache 2 should have no stale local TXs";
   }

   /**
    * Tests that setting a cacheModeLocal=true flag prevents propagation of the putForExternalRead().
    *
    * @throws Exception
    */
   private void cacheModeLocalTest(boolean transactional) throws Exception {
      RpcManager rpcManager = EasyMock.createMock(RpcManager.class);
      RpcManager originalRpcManager = TestingUtil.replaceComponent(cache1.getCacheManager(), RpcManager.class, rpcManager, true);
      try {

         // specify that we expectWithTx nothing will be called on the mock Rpc Manager.
         replay(rpcManager);

         // now try a simple replication.  Since the RpcManager is a mock object it will not actually replicate anything.
         if (transactional)
            tm1.begin();

         cache1.getAdvancedCache().putForExternalRead(key, value, Flag.CACHE_MODE_LOCAL);

         if (transactional)
            tm1.commit();

         verify(rpcManager);
      } finally {
         if (originalRpcManager != null) {
            // cleanup
            TestingUtil.replaceComponent(cache1.getCacheManager(), RpcManager.class, originalRpcManager, true);
            cache1.remove(key);
         }
      }
   }
}
