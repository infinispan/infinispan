package org.infinispan.remoting.jgroups;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
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
@Test(groups = "unstable", testName = "remoting.jgroups.MissingRpcDispatcherTest",
      description = "See ISPN-4034. Disabled because I removed the cache members filter in 5.2 -- original group: functional")
@CleanupAfterMethod
public class MissingRpcDispatcherTest extends MultipleCacheManagersTest {
   protected String cacheName = "replSync";
   protected CacheMode cacheMode = CacheMode.REPL_SYNC;

   @Override
   protected void createCacheManagers() throws Exception {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(cacheMode, false);
      c.clustering().stateTransfer().fetchInMemoryState(true);
      createClusteredCaches(1, cacheName, c);
   }

   public void testExtraChannelWithoutRpcDispatcher() throws Exception {
      // start with a single cache
      Cache<String, String> cache1 = cache(0, cacheName);
      cache1.put("k1", "v1");
      assert "v1".equals(cache1.get("k1"));

      // create a new jgroups channel that will join the cluster
      // but without attaching the Infinispan RpcDispatcher
      Channel channel2 = createJGroupsChannel(manager(0).getCacheManagerConfiguration());
      try {
         // try the put operation again
         cache1.put("k2", "v2");
         assert "v2".equals(cache1.get("k2"));

         // create a new cache, make sure it joins properly
         ConfigurationBuilder c = getDefaultClusteredCacheConfig(cacheMode, false);
         c.clustering().stateTransfer().fetchInMemoryState(true);
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(new TransportFlags());
         cm.defineConfiguration(cacheName, c.build());
         Cache<String, String> cache2 = cm.getCache(cacheName);
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

   private Channel createJGroupsChannel(GlobalConfiguration oldGC) {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder().read(oldGC);
      TestCacheManagerFactory.amendTransport(builder);
      GlobalConfiguration gc = builder.build();
      Properties p = gc.transport().properties();
      String jgroupsCfg = p.getProperty(JGroupsTransport.CONFIGURATION_STRING);
      try {
         JChannel channel = new JChannel(jgroupsCfg);
         channel.setName(gc.transport().nodeName());
         channel.connect(gc.transport().clusterName());
         return channel;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }
}
