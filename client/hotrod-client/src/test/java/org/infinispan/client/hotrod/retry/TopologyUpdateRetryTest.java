package org.infinispan.client.hotrod.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.marshall;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.test.NoopChannelOperation;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.Test;

import io.netty.channel.Channel;

@Test(groups = "functional", testName = "client.hotrod.retry.TopologyUpdateRetryTest")
public class TopologyUpdateRetryTest extends AbstractRetryTest {

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
   }

   public void testTopologyChangeWithQueuedOperations() throws Exception {
      InetSocketAddress address = (InetSocketAddress) channelFactory.getConsistentHash(RemoteCacheManager.cacheNameBytes())
            .getServer(marshall(1));

      // We acquire the channel and never release. All the issues operations will queue up.
      Channel channel = channelFactory.fetchChannelAndInvoke(address, new NoopChannelOperation()).get(10, TimeUnit.SECONDS);

      // We issue all of these operations which do not complete until the latch is released.
      AggregateCompletionStage<?> operations = CompletionStages.aggregateCompletionStage();
      for (int i = 0; i < 10; i++) {
         operations.dependsOn(remoteCache.putAsync(1, "v" + i));
      }

      // While all operations are stuck. A new topology arrives!!
      CacheTopologyInfo currentInfo = channelFactory.getCacheTopologyInfo(RemoteCacheManager.cacheNameBytes());
      Collection<InetSocketAddress> servers = channelFactory.getServers();
      servers.remove(address);
      InetSocketAddress[] newServers = servers.toArray(new InetSocketAddress[0]);
      SocketAddress[][] owners = new SocketAddress[256][];
      for (int i = 0; i < 256; i++) {
         owners[i] = newServers;
      }

      // The topology update will close all the enqueued operations.
      channelFactory.receiveTopology(RemoteCacheManager.cacheNameBytes(), channelFactory.getTopologyAge(),
            currentInfo.getTopologyId() + 1, newServers, owners, (short) 3);

      // We do not have active channel for the current topology.
      eventually(() -> channelFactory.getNumActive() == 0);

      // We did not stop the server, so the channel to the old server is still active.
      assertThat(channel.isActive()).isTrue();

      // Releasing to a terminated pool closes the channel.
      ChannelRecord.of(channel).release(channel);

      // The retry will kick and the operations complete successfully.
      operations.freeze().toCompletableFuture().get(10, TimeUnit.SECONDS);

      // The channel then closes!
      eventually(() -> !channel.isActive());
   }
}
