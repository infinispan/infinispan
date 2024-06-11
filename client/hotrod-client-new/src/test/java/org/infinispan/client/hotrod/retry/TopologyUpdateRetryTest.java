package org.infinispan.client.hotrod.retry;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.marshall;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.OperationChannel;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.retry.TopologyUpdateRetryTest")
public class TopologyUpdateRetryTest extends AbstractRetryTest {

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
   }

   public void testTopologyChangeWithQueuedOperations() throws Exception {
      InetSocketAddress address = (InetSocketAddress) dispatcher.getConsistentHash(HotRodConstants.DEFAULT_CACHE_NAME)
            .getServer(marshall(1));

      // We acquire the channel and never release. All the issues operations will queue up.
      OperationChannel opChannel = dispatcher.getHandlerForAddress(address);

      CountDownLatch latch = new CountDownLatch(1);

      // Block the event loop which will prevent the operations from being processed
      opChannel.getChannel().eventLoop().submit(() -> latch.await(10, TimeUnit.SECONDS));

      // We issue all of these operations which do not complete until the latch is released.
      AggregateCompletionStage<?> operations = CompletionStages.aggregateCompletionStage();
      for (int i = 0; i < 10; i++) {
         operations.dependsOn(remoteCache.putAsync(1, "v" + i));
      }

      // While all operations are stuck. A new topology arrives!!
      CacheTopologyInfo currentInfo = dispatcher.getCacheTopologyInfo(HotRodConstants.DEFAULT_CACHE_NAME);
      Collection<InetSocketAddress> servers = dispatcher.getServers();
      servers.remove(address);
      InetSocketAddress[] newServers = servers.toArray(new InetSocketAddress[0]);
      SocketAddress[][] owners = new SocketAddress[256][];
      for (int i = 0; i < 256; i++) {
         owners[i] = newServers;
      }

      // The topology update will close all the enqueued operations.
      dispatcher.updateTopology(HotRodConstants.DEFAULT_CACHE_NAME, null,
            currentInfo.getTopologyId() + 1, newServers, owners, (short) 3);

      latch.countDown();

      // The retry will kick and the operations complete successfully.
      operations.freeze().toCompletableFuture().get(10, TimeUnit.SECONDS);
   }
}
