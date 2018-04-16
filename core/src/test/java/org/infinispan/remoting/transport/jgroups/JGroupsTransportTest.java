package org.infinispan.remoting.transport.jgroups;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.ByteString;
import org.jgroups.util.UUID;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @since 9.0
 */
@Test
public class JGroupsTransportTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
      createCluster(configurationBuilder, 2);
   }

   public void testSynchronousIgnoreLeaversInvocationToNonMembers() throws Exception {
      UUID randomUuid = UUID.randomUUID();
      Address randomAddress = JGroupsAddressCache.fromJGroupsAddress(randomUuid);

      JGroupsTransport transport = (JGroupsTransport) manager(0).getTransport();
      long initialMessages = transport.getChannel().getSentMessages();
      ReplicableCommand command = new ClusteredGetCommand("key", ByteString.fromString("cache"), 0);
      CompletableFuture<Map<Address, Response>> future = transport
            .invokeRemotelyAsync(Collections.singletonList(randomAddress), command,
                                 ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, 1, null, DeliverOrder.NONE, true);
      assertEquals(CacheNotFoundResponse.INSTANCE, future.get().get(randomAddress));
      assertEquals(initialMessages, transport.getChannel().getSentMessages());
   }
}
