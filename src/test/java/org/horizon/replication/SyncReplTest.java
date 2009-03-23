/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.horizon.replication;

import static org.easymock.EasyMock.*;
import org.horizon.Cache;
import org.horizon.commands.CacheRPCCommand;
import org.horizon.config.Configuration;
import org.horizon.remoting.RPCManager;
import org.horizon.remoting.RPCManagerImpl;
import org.horizon.remoting.ResponseFilter;
import org.horizon.remoting.ResponseMode;
import org.horizon.remoting.transport.Address;
import org.horizon.remoting.transport.Transport;
import org.horizon.test.MultipleCacheManagersTest;
import org.horizon.test.TestingUtil;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
@Test(groups = "functional", testName = "replication.SyncReplTest")
public class SyncReplTest extends MultipleCacheManagersTest {
   Cache cache1, cache2;
   String k = "key", v = "value";

   protected void createCacheManagers() throws Throwable {
      Configuration replSync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      createClusteredCaches(2, "replSync", replSync);

      cache1 = manager(0).getCache("replSync");
      cache2 = manager(1).getCache("replSync");
   }

   public void testBasicOperation() {
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      assertNull("Should be null", cache1.get(k));
      assertNull("Should be null", cache2.get(k));

      cache1.put(k, v);

      assertEquals(v, cache1.get(k));
      assertEquals("Should have replicated", v, cache2.get(k));

      cache2.remove(k);
      assert cache1.isEmpty();
      assert cache2.isEmpty();
   }

   public void testMultpleCachesOnSharedTransport() {
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);
      assert cache1.isEmpty();
      assert cache2.isEmpty();

      Configuration newConf = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      defineCacheOnAllManagers("newCache", newConf);
      Cache altCache1 = manager(0).getCache("newCache");
      Cache altCache2 = manager(1).getCache("newCache");

      try {
         assert altCache1.isEmpty();
         assert altCache2.isEmpty();

         cache1.put(k, v);
         assert cache1.get(k).equals(v);
         assert cache2.get(k).equals(v);
         assert altCache1.isEmpty();
         assert altCache2.isEmpty();

         altCache1.put(k, "value2");
         assert altCache1.get(k).equals("value2");
         assert altCache2.get(k).equals("value2");
         assert cache1.get(k).equals(v);
         assert cache2.get(k).equals(v);
      } finally {
         removeCacheFromCluster("newCache");
      }
   }

   public void testReplicateToNonExistentCache() {
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);
      assert cache1.isEmpty();
      assert cache2.isEmpty();

      Configuration newConf = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      defineCacheOnAllManagers("newCache2", newConf);
      Cache altCache1 = manager(0).getCache("newCache2");

      try {
         assert altCache1.isEmpty();

         cache1.put(k, v);
         assert cache1.get(k).equals(v);
         assert cache2.get(k).equals(v);
         assert altCache1.isEmpty();

         altCache1.put(k, "value2");
         assert altCache1.get(k).equals("value2");
         assert cache1.get(k).equals(v);
         assert cache2.get(k).equals(v);

         assert manager(0).getCache("newCache2").get(k).equals("value2");
      } finally {
         removeCacheFromCluster("newCache2");
      }
   }

   public void testMixingSyncAndAsyncOnSameTransport() throws Exception {
      Transport originalTransport = null;
      RPCManagerImpl rpcManager = null;
      try {
         Configuration asyncCache = getDefaultClusteredConfig(Configuration.CacheMode.REPL_ASYNC);
         defineCacheOnAllManagers("asyncCache", asyncCache);
         Cache asyncCache1 = manager(0).getCache("asyncCache");

         // replace the transport with a mock object
         Transport mockTransport = createMock(Transport.class);
         Address mockAddressOne = createNiceMock(Address.class);
         Address mockAddressTwo = createNiceMock(Address.class);
         List<Address> addresses = new LinkedList<Address>();
         addresses.add(mockAddressOne);
         addresses.add(mockAddressTwo);
         expect(mockTransport.getAddress()).andReturn(mockAddressOne).anyTimes();
         expect(mockTransport.getMembers()).andReturn(addresses).anyTimes();
         replay(mockAddressOne, mockAddressTwo);

         // this is shared by all caches managed by the cache manager
         originalTransport = TestingUtil.extractComponent(asyncCache1, Transport.class);
         rpcManager = (RPCManagerImpl) TestingUtil.extractComponent(asyncCache1, RPCManager.class);
         rpcManager.setTransport(mockTransport);

         expect(mockTransport.invokeRemotely((List<Address>) anyObject(), (CacheRPCCommand) anyObject(), eq(ResponseMode.SYNCHRONOUS),
                                             anyLong(), anyBoolean(), (ResponseFilter) anyObject(), anyBoolean()))
               .andReturn(Collections.emptyList()).once();

         replay(mockTransport);
         // check that the replication call was sync
         cache1.put("k", "v");

         // reset to test for async
         reset(mockTransport);
         expect(mockTransport.getAddress()).andReturn(mockAddressOne).anyTimes();
         expect(mockTransport.getMembers()).andReturn(addresses).anyTimes();
         expect(mockTransport.invokeRemotely((List<Address>) anyObject(), (CacheRPCCommand) anyObject(), eq(ResponseMode.ASYNCHRONOUS),
                                             anyLong(), anyBoolean(), (ResponseFilter) anyObject(), anyBoolean()))
               .andReturn(Collections.emptyList()).once();

         replay(mockTransport);
         asyncCache1.put("k", "v");
         // check that the replication call was async
         verify(mockTransport);
      } finally {
         // replace original transport
         if (rpcManager != null) rpcManager.setTransport(originalTransport);
      }
   }
}
