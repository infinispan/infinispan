package org.infinispan.remoting;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "remoting.SingleClusterMemberTransportInvokeTest")
public class SingleClusterMemberTransportInvokeTest extends MultipleCacheManagersTest {
   private final String key = "k", value = "v", value2 = "v2";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      createClusteredCaches(1, "replSync", c);
   }

   public void testInvokeRemotelyWhenSingleMember() throws Exception {
      Cache cache1 = cache(0, "replSync");
      Transport mockTransport = mock(Transport.class);
      RpcManagerImpl rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache1, RpcManager.class);
      Transport originalTransport = TestingUtil.extractComponent(cache1, Transport.class);
      try {
         Address mockAddress1 = mock(Address.class);
         List<Address> memberList = new ArrayList<Address>(1);
         memberList.add(mockAddress1);
         when(mockTransport.getMembers()).thenReturn(memberList);
         when(mockTransport.getAddress()).thenReturn(null);
         rpcManager.setTransport(mockTransport);
         // Transport invoke remote should not be called.
         // now try a simple replication.  Since the RpcManager is a mock object it will not actually replicate anything.
         cache1.put(key, value);
      } finally {
         if (rpcManager != null) rpcManager.setTransport(originalTransport);
      }
   }
}
