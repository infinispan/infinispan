package org.infinispan.remoting.jgroups;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * When the JGroups channel is started externally and injected via {@code ChannelLookup},
 * there is a small window where incoming messages will be silently discarded:
 * between the time the channel is started externally and the time JGroupsTransport attaches the RpcDispatcher.
 *
 * In replication mode a put operation would wait for a success response from all the members
 * of the cluster, and if the RPC was initiated during this time window it would never get a response.
 *
 * This test checks that the caller doesn't get a (@code TimeoutException} waiting for a response.
 *
 * @since 5.1
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 */
@Test(groups = "functional", testName = "remoting.jgroups.MissingRpcDispatcherTest",
      enabled = false, description = "Temporarily disabled because I removed the cache members filter in 5.2")
@CleanupAfterMethod
public class MissingRpcDispatcherTest extends MultipleCacheManagersTest {
   protected String cacheName = "replSync";
   protected Configuration.CacheMode cacheMode = Configuration.CacheMode.REPL_SYNC;

   @Override
   protected void createCacheManagers() throws Exception {
      Configuration c = getDefaultClusteredConfig(cacheMode);
      c.fluent()
            .clustering().stateRetrieval().fetchInMemoryState(true);
      createClusteredCaches(1, cacheName, c);
   }

   public void testExtraChannelWithoutRpcDispatcher() throws Exception {
      // start with a single cache
      Cache cache1 = cache(0, cacheName);
      cache1.put("k1", "v1");
      assert "v1".equals(cache1.get("k1"));

      // create a new jgroups channel that will join the cluster
      // but without attaching the Infinispan RpcDispatcher
      Channel channel2 = createJGroupsChannel(manager(0).getGlobalConfiguration());
      try {
         // try the put operation again
         cache1.put("k2", "v2");
         assert "v2".equals(cache1.get("k2"));

         // create a new cache, make sure it joins properly
         Configuration c = getDefaultClusteredConfig(cacheMode);
         c.fluent()
               .clustering().stateRetrieval().fetchInMemoryState(true);
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(new TransportFlags());
         cm.defineConfiguration(cacheName, c);
         Cache cache2 = cm.getCache(cacheName);
         assert cache2.getAdvancedCache().getRpcManager().getTransport().getMembers().size() == 3;

         assert "v1".equals(cache1.get("k1"));
         assert "v2".equals(cache1.get("k2"));
         cache1.put("k1", "v1_2");
         cache2.put("k2", "v2_2");
         assert "v1_2".equals(cache1.get("k1"));
         assert "v2_2".equals(cache1.get("k2"));
      } finally {
         channel2.close();
      }
   }

   private Channel createJGroupsChannel(GlobalConfiguration gc) {
      GlobalConfiguration newGC = gc.clone();
      TestCacheManagerFactory.amendTransport(newGC);
      Properties p = newGC.getTransportProperties();
      String jgroupsCfg = p.getProperty(JGroupsTransport.CONFIGURATION_STRING);
      try {
         JChannel channel = new JChannel(jgroupsCfg);
         channel.setName(newGC.getTransportNodeName());
         channel.connect(newGC.getClusterName());
         return channel;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }
}
