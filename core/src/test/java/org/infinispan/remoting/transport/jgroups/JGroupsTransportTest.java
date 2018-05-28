package org.infinispan.remoting.transport.jgroups;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.jgroups.util.UUID;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @since 9.0
 */
@Test(groups = "unit", testName = "remoting.transport.jgroups.JGroupsTransportTest")
public class JGroupsTransportTest extends MultipleCacheManagersTest {

   public static final ByteString CACHE_NAME = ByteString.fromString("cache");

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
      ReplicableCommand command = new ClusteredGetCommand("key", CACHE_NAME, 0, 0);
      CompletableFuture<Map<Address, Response>> future = transport
            .invokeRemotelyAsync(Collections.singletonList(randomAddress), command,
                                 ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, 1, null, DeliverOrder.NONE, true);
      assertEquals(CacheNotFoundResponse.INSTANCE, future.get().get(randomAddress));
      assertEquals(initialMessages, transport.getChannel().getSentMessages());
   }

   public void testInvokeCommandStaggeredToNonMember() throws Exception {
      UUID randomUuid = UUID.randomUUID();
      Address randomAddress = JGroupsAddressCache.fromJGroupsAddress(randomUuid);

      // Send message only to non-member
      JGroupsTransport transport = (JGroupsTransport) manager(0).getTransport();
      ReplicableCommand command = new ClusteredGetCommand("key", CACHE_NAME, 0, 0);
      CompletionStage<Map<Address, Response>> future =
         transport.invokeCommandStaggered(Collections.singletonList(randomAddress), command,
                                          MapResponseCollector.ignoreLeavers(), DeliverOrder.NONE, 5,
                                          TimeUnit.SECONDS);
      assertEquals(Collections.singletonMap(randomAddress, CacheNotFoundResponse.INSTANCE),
                   future.toCompletableFuture().get());

      // Send message to view member that doesn't have the cache and to non-member
      CompletionStage<Map<Address, Response>> future2 =
         transport.invokeCommandStaggered(Arrays.asList(address(1), randomAddress), command,
                                          MapResponseCollector.ignoreLeavers(), DeliverOrder.NONE, 5,
                                          TimeUnit.SECONDS);
      Map<Object, Object> expected = TestingUtil.mapOf(address(1), CacheNotFoundResponse.INSTANCE,
                                                       randomAddress, CacheNotFoundResponse.INSTANCE);
      assertEquals(expected, future2.toCompletableFuture().get());

      // Send message to view member that doesn't have the cache and to non-member
      // and block the response from the view member
      CompletableFuture<Void> blocker = blockRemoteGets();
      try {
         CompletionStage<Map<Address, Response>> future3 =
            transport.invokeCommandStaggered(Arrays.asList(address(1), randomAddress), command,
                                             MapResponseCollector.ignoreLeavers(), DeliverOrder.NONE, 5,
                                             TimeUnit.SECONDS);
         // Wait for the stagger timeout (5s / 10 / 2) to expire before sending a reply back
         Thread.sleep(500);
         blocker.complete(null);
         assertEquals(expected, future3.toCompletableFuture().get());
      } finally {
         blocker.complete(null);
      }
   }

   private CompletableFuture<Void> blockRemoteGets() {
      CompletableFuture<Void> blocker = new CompletableFuture<>();
      InboundInvocationHandler oldInvocationHandler = TestingUtil.extractGlobalComponent(manager(1),
                                                                                         InboundInvocationHandler
                                                                                            .class);
      InboundInvocationHandler blockingInvocationHandler = new InboundInvocationHandler() {
         @Override
         public void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
            if (command instanceof ClusteredGetCommand) {
               log.tracef("Blocking clustered get");
               blocker.thenRun(() -> oldInvocationHandler.handleFromCluster(origin, command, reply, order));
            } else {
               oldInvocationHandler.handleFromCluster(origin, command, reply, order);
            }
         }

         @Override
         public void handleFromRemoteSite(String origin, XSiteReplicateCommand command, Reply reply,
                                          DeliverOrder order) {
            oldInvocationHandler.handleFromRemoteSite(origin, command, reply, order);
         }
      };
      TestingUtil.replaceComponent(manager(1), InboundInvocationHandler.class, blockingInvocationHandler, true);
      return blocker;
   }
}
