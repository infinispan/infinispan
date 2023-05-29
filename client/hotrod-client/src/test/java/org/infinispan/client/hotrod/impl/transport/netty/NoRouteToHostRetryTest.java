package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.retry.AbstractRetryTest;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.jboss.byteman.contrib.bmunit.BMNGListener;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import io.netty.channel.Channel;

@Listeners(BMNGListener.class)
@Test(groups = "functional", testName = "client.hotrod.retry.NoRouteToHostRetryTest")
public class NoRouteToHostRetryTest extends AbstractRetryTest {
   public static final AtomicBoolean FAIL = new AtomicBoolean(true);

   public NoRouteToHostRetryTest() {
      super();
      this.nbrOfServers = 1;
   }

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.clustering().hash().numOwners(1);
      return builder;
   }

   @Override
   protected void amendRemoteCacheManagerConfiguration(org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder) {
      // Start with this, after exception we will switch to BASIC.
      builder.clientIntelligence(ClientIntelligence.HASH_DISTRIBUTION_AWARE)
            .connectionPool().maxActive(5);
   }

   @TestForIssue(jiraKey = "ISPN-14727")
   @Test
   @BMRule(name = "Throw when connecting",
         targetClass = "io.netty.channel.epoll.AbstractEpollChannel",
         targetMethod = "doConnect0",
         condition = "org.infinispan.client.hotrod.impl.transport.netty.NoRouteToHostRetryTest.FAIL.get()",
         action = "throw new java.net.NoRouteToHostException(\"No route to host\");")
   public void testIntelligenceSwitchToBasic() throws Exception {
      // The start of the cache manager will send a ping to all servers.
      // That would create a channel *before* the rule is applied.
      // We use reflection to extract the existing channels and close them.
      ChannelFactory channelFactory = remoteCacheManager.getChannelFactory();
      Map<SocketAddress, ChannelPool> channelPoolMap = TestingUtil.extractField(channelFactory, "channelPoolMap");
      InetSocketAddress address = InetSocketAddress.createUnresolved(hotRodServer1.getHost(), hotRodServer1.getPort());
      ChannelPool pool = channelPoolMap.get(address);
      Deque<Channel> channels = TestingUtil.extractField(pool, "channels");
      for (Channel ch : channels) {
         ch.close().awaitUninterruptibly();
      }
      channels.clear();

      // Assert we have HASH_DISTRIBUTION_AWARE intelligence.
      assertEquals(channelFactory.getClientIntelligence(), ClientIntelligence.HASH_DISTRIBUTION_AWARE);

      // This operation will create a new channel, which fails to connect with a NoRouteToHostException.
      // That will trigger the fallback to BASIC and the operation retries and succeed.
      Exceptions.expectException(TransportException.class, ".*NoRouteToHostException: No route to host: .*$", () -> remoteCache.put("key", "value"));
      FAIL.set(false);

      remoteCache.put("key", "correct");
      assertEquals(remoteCache.get("key"), "correct");

      // After the operation, the intelligence should be switched to BASIC.
      assertEquals(channelFactory.getClientIntelligence(), ClientIntelligence.BASIC);
   }
}
