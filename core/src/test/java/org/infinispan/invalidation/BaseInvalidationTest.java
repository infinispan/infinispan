package org.infinispan.invalidation;

import static org.easymock.EasyMock.*;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.CacheRpcCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.config.Configuration;
import org.infinispan.invocation.Flag;
import org.infinispan.remoting.RpcManager;
import org.infinispan.remoting.RpcManagerImpl;
import org.infinispan.remoting.ResponseFilter;
import org.infinispan.remoting.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.DummyTransactionManagerLookup;
import static org.testng.AssertJUnit.*;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.List;

@Test(groups = "functional")
public abstract class BaseInvalidationTest extends MultipleCacheManagersTest {
   protected AdvancedCache cache1, cache2;
   protected boolean isSync;

   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(isSync ? Configuration.CacheMode.INVALIDATION_SYNC : Configuration.CacheMode.INVALIDATION_ASYNC);
      c.setStateRetrievalTimeout(1000);
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      List<Cache> caches = createClusteredCaches(2, "invalidation", c);
      cache1 = caches.get(0).getAdvancedCache();
      cache2 = caches.get(1).getAdvancedCache();
   }

   public void testRemove() throws Exception {
      cache1.put("key", "value", Flag.CACHE_MODE_LOCAL);
      assertEquals("value", cache1.get("key"));
      cache2.put("key", "value", Flag.CACHE_MODE_LOCAL);
      assertEquals("value", cache2.get("key"));

      replListener(cache2).expectAny();
      assertEquals("value", cache1.remove("key"));
      replListener(cache2).waitForRPC();

      assertEquals(false, cache2.containsKey("key"));
   }

   public void testResurrectEntry() throws Exception {
      replListener(cache2).expect(InvalidateCommand.class);
      cache1.put("key", "value");
      replListener(cache2).waitForRPC();

      assertEquals("value", cache1.get("key"));
      assertEquals(null, cache2.get("key"));
      replListener(cache2).expect(InvalidateCommand.class);
      cache1.put("key", "newValue");
      replListener(cache2).waitForRPC();

      assertEquals("newValue", cache1.get("key"));
      assertEquals(null, cache2.get("key"));

      replListener(cache2).expect(InvalidateCommand.class);
      assertEquals("newValue", cache1.remove("key"));
      replListener(cache2).waitForRPC();

      assertEquals(null, cache1.get("key"));
      assertEquals(null, cache2.get("key"));

      // Restore locally
      replListener(cache2).expect(InvalidateCommand.class);
      cache1.put("key", "value");
      replListener(cache2).waitForRPC();

      assertEquals("value", cache1.get("key"));
      assertEquals(null, cache2.get("key"));

      replListener(cache1).expect(InvalidateCommand.class);
      cache2.put("key", "value2");
      replListener(cache1).waitForRPC();

      assertEquals("value2", cache2.get("key"));
      assertEquals(null, cache1.get("key"));
   }

   public void testDeleteNonExistentEntry() throws Exception {
      assertNull("Should be null", cache1.get("key"));
      assertNull("Should be null", cache2.get("key"));

      replListener(cache2).expect(InvalidateCommand.class);
      cache1.put("key", "value");
      replListener(cache2).waitForRPC();

      assertEquals("value", cache1.get("key"));
      assertNull("Should be null", cache2.get("key"));

      // OK, here's the real test
      TransactionManager tm = TestingUtil.getTransactionManager(cache2);
      replListener(cache1).expect(InvalidateCommand.class); // invalidates always happen outside of a tx
      tm.begin();
      // Remove an entry that doesn't exist in cache2
      cache2.remove("key");
      tm.commit();
      replListener(cache1).waitForRPC();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testTxSyncUnableToInvalidate() throws Exception {
      replListener(cache2).expect(InvalidateCommand.class);
      cache1.put("key", "value");
      replListener(cache2).waitForRPC();

      assertEquals("value", cache1.get("key"));
      assertNull(cache2.get("key"));

      // start a tx that cacahe1 will have to send out an evict ...
      TransactionManager mgr1 = TestingUtil.getTransactionManager(cache1);
      TransactionManager mgr2 = TestingUtil.getTransactionManager(cache2);

      replListener(cache1).expect(InvalidateCommand.class);
      replListener(cache2).expect(InvalidateCommand.class);
      mgr1.begin();
      cache1.put("key", "value2");
      Transaction tx1 = mgr1.suspend();
      mgr2.begin();
      cache2.put("key", "value3");
      Transaction tx2 = mgr2.suspend();
      mgr1.resume(tx1);
      // this oughtta fail
      try {
         mgr1.commit();
         if (isSync) {
            fail("Ought to have failed!");
         } else {
            assert true : "Ought to have succeeded";
//            replListener(cache2).waitForRPC();
         }
      }
      catch (RollbackException roll) {
         if (isSync)
            assertTrue("Ought to have failed!", true);
         else
            fail("Ought to have succeeded!");
      }

      mgr2.resume(tx2);
      try {
         mgr2.commit();
         replListener(cache1).waitForRPC();
         if (!isSync) replListener(cache2).waitForRPC();
         assertTrue("Ought to have succeeded!", true);
      }
      catch (RollbackException roll) {
         fail("Ought to have succeeded!");
      }
   }

   public void testCacheMode() throws Exception {
      RpcManagerImpl rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache1, RpcManager.class);
      Transport origTransport = TestingUtil.extractComponent(cache1, Transport.class);
      try {
         Transport mockTransport = createMock(Transport.class);
         rpcManager.setTransport(mockTransport);
         Address addressOne = createNiceMock(Address.class);
         Address addressTwo = createNiceMock(Address.class);
         List<Address> members = new ArrayList<Address>(2);
         members.add(addressOne);
         members.add(addressTwo);

         expect(mockTransport.getMembers()).andReturn(members).anyTimes();
         expect(mockTransport.getAddress()).andReturn(addressOne).anyTimes();
         expect(mockTransport.invokeRemotely((List<Address>) anyObject(), (CacheRpcCommand) anyObject(),
                                             eq(isSync ? ResponseMode.SYNCHRONOUS : ResponseMode.ASYNCHRONOUS),
                                             anyLong(), anyBoolean(), (ResponseFilter) anyObject(), anyBoolean())).andReturn(null).anyTimes();
         replay(mockTransport);

         cache1.put("k", "v");
         verify(mockTransport);

      } finally {
         if (rpcManager != null) rpcManager.setTransport(origTransport);
      }
   }

   public void testPutIfAbsent() {
      assert null == cache2.put("key", "value", Flag.CACHE_MODE_LOCAL);
      assert cache2.get("key").equals("value");
      assert cache1.get("key") == null;

      replListener(cache2).expect(InvalidateCommand.class);
      cache1.putIfAbsent("key", "value");
      replListener(cache2).waitForRPC();

      assert cache1.get("key").equals("value");
      assert cache2.get("key") == null;

      assert null == cache2.put("key", "value2", Flag.CACHE_MODE_LOCAL);

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value2");

      cache1.putIfAbsent("key", "value3");

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value2"); // should not invalidate cache2!!
   }

   public void testRemoveIfPresent() {
      cache1.put("key", "value1", Flag.CACHE_MODE_LOCAL);
      cache2.put("key", "value2", Flag.CACHE_MODE_LOCAL);
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      assert !cache1.remove("key", "value");

      assert cache1.get("key").equals("value1") : "Should not remove";
      assert cache2.get("key").equals("value2") : "Should not evict";

      replListener(cache2).expect(InvalidateCommand.class);
      cache1.remove("key", "value1");
      replListener(cache2).waitForRPC();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testClear() {
      cache1.put("key", "value1", Flag.CACHE_MODE_LOCAL);
      cache2.put("key", "value2", Flag.CACHE_MODE_LOCAL);
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      replListener(cache2).expect(ClearCommand.class);
      cache1.clear();
      replListener(cache2).waitForRPC();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testReplace() {
      cache2.put("key", "value2", Flag.CACHE_MODE_LOCAL);
      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      assert null == cache1.replace("key", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      assert null == cache1.put("key", "valueN", Flag.CACHE_MODE_LOCAL);

      replListener(cache2).expect(InvalidateCommand.class);
      cache1.replace("key", "value1");
      replListener(cache2).waitForRPC();

      assert cache1.get("key").equals("value1");
      assert cache2.get("key") == null;
   }

   public void testReplaceWithOldVal() {
      cache2.put("key", "value2", Flag.CACHE_MODE_LOCAL);
      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      assert !cache1.replace("key", "valueOld", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      assert null == cache1.put("key", "valueN", Flag.CACHE_MODE_LOCAL);

      assert !cache1.replace("key", "valueOld", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key").equals("valueN");
      assert cache2.get("key").equals("value2");

      replListener(cache2).expect(InvalidateCommand.class);
      assert cache1.replace("key", "valueN", "value1");
      replListener(cache2).waitForRPC();

      assert cache1.get("key").equals("value1");
      assert cache2.get("key") == null;
   }
}
