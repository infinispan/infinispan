package org.infinispan.replication;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ReplicatedControlledConsistentHashFactory;
import org.testng.annotations.Test;

/**
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
@Test(groups = "functional", testName = "replication.SyncReplTest")
public class SyncReplTest extends MultipleCacheManagersTest {

   private String k = "key", v = "value";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder replSync = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      replSync.clustering().hash().numSegments(1).consistentHashFactory(new ReplicatedControlledConsistentHashFactory(0));
      createClusteredCaches(2, ReplicatedControlledConsistentHashFactory.SCI.INSTANCE, "replSync", replSync);
   }

   public void testBasicOperation() {
      Cache<String, String> cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");

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
      Cache<String, String> cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");

      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);
      assert cache1.isEmpty();
      assert cache2.isEmpty();

      ConfigurationBuilder newConf = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      defineConfigurationOnAllManagers("newCache", newConf);
      Cache<String, String> altCache1 = manager(0).getCache("newCache");
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
      // strictPeerToPeer is now disabled by default
      boolean strictPeerToPeer = false;

      Cache<String, String> cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);
      assert cache1.isEmpty();
      assert cache2.isEmpty();

      ConfigurationBuilder newConf = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);

      defineConfigurationOnAllManagers("newCache2", newConf);
      Cache<String, String> altCache1 = manager(0).getCache("newCache2");

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
      Cache<String, String> cache1 = cache(0, "replSync");
      cache(1, "replSync");
      waitForClusterToForm("replSync");

      Transport originalTransport = null;
      RpcManagerImpl rpcManager = null;
      RpcManagerImpl asyncRpcManager = null;
      try {
         ConfigurationBuilder asyncCache = getDefaultClusteredCacheConfig(CacheMode.REPL_ASYNC, false);
         asyncCache.clustering().hash().numSegments(1).consistentHashFactory(new ReplicatedControlledConsistentHashFactory(0));
         defineConfigurationOnAllManagers("asyncCache", asyncCache);
         Cache<String, String> asyncCache1 = manager(0).getCache("asyncCache");
         manager(1).getCache("asyncCache");
         waitForClusterToForm("asyncCache");

         // this is shared by all caches managed by the cache manager
         originalTransport = TestingUtil.extractGlobalComponent(cache1.getCacheManager(), Transport.class);
         Transport mockTransport = spy(originalTransport);

         // replace the transport with a mock object
         rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache1, RpcManager.class);
         rpcManager.setTransport(mockTransport);

         // check that the replication call was sync
         cache1.put("k", "v");
         verify(mockTransport)
               .invokeCommandOnAll(any(), any(), any(), any(), anyLong(), any());

         // resume to test for async
         asyncRpcManager = (RpcManagerImpl) TestingUtil.extractComponent(asyncCache1, RpcManager.class);
         asyncRpcManager.setTransport(mockTransport);

         reset(mockTransport);

         asyncCache1.put("k", "v");
         verify(mockTransport).sendToAll(any(ReplicableCommand.class), any(DeliverOrder.class));
      } finally {
         // replace original transport
         if (rpcManager != null)
            rpcManager.setTransport(originalTransport);
         if (asyncRpcManager != null)
            asyncRpcManager.setTransport(originalTransport);
      }
   }
}
