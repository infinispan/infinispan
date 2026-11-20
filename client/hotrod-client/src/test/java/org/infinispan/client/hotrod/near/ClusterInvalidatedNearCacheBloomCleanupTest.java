package org.infinispan.client.hotrod.near;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.ClusterInvalidatedNearCacheBloomCleanupTest")
public class ClusterInvalidatedNearCacheBloomCleanupTest extends AbstractInfinispanTest {
   public void testUnexpectedEventStillStopsResources() throws Throwable {
      Fixture fixture = new Fixture();
      try {
         fixture.createCacheManagers();
         List<RemoteCacheManager> clients = fixture.clientSnapshot();
         List<NettyTransport> transports = fixture.transportSnapshot();
         List<EmbeddedCacheManager> managers = fixture.managerSnapshot();
         fixture.assertClients.forEach(AssertsNearCache::resetEvents);
         fixture.client0.events.add(new MockNearCacheService.MockRemoveEvent<>(0));

         AssertionError failure = assertThrows(AssertionError.class, fixture::destroy);
         assertTrue(failure.getMessage().contains("MockRemoveEvent{key=0}"));
         clients.forEach(client -> assertFalse(client.isStarted()));
         transports.forEach(transport -> assertFalse(transport.isRunning()));
         managers.forEach(manager -> assertTrue(manager.getStatus() == ComponentStatus.TERMINATED));
         assertTrue(fixture.assertClients.isEmpty());
      } finally {
         // Also release resources when running this regression against the broken teardown.
         fixture.assertClients.forEach(AssertsNearCache::resetEvents);
         fixture.destroy();
      }
   }

   public void testClientStopFailureDoesNotSkipRemainingCleanup() {
      Fixture fixture = new Fixture();
      RemoteCacheManager failingClient = mock(RemoteCacheManager.class);
      RemoteCacheManager otherClient = mock(RemoteCacheManager.class);
      HotRodServer server = mock(HotRodServer.class);
      doThrow(new IllegalStateException("Injected stop failure")).when(failingClient).stop();
      fixture.addResources(failingClient, otherClient, server);

      fixture.destroy();

      verify(failingClient).stop();
      verify(otherClient).stop();
      verify(server).stop();
      assertTrue(fixture.clientSnapshot().isEmpty());
   }

   private static class Fixture extends ClusterInvalidatedNearCacheBloomTest {
      List<RemoteCacheManager> clientSnapshot() {
         return List.copyOf(clients);
      }

      List<NettyTransport> transportSnapshot() {
         return servers.stream().map(HotRodServer::getTransport).toList();
      }

      List<EmbeddedCacheManager> managerSnapshot() {
         return List.copyOf(cacheManagers);
      }

      void addResources(RemoteCacheManager first, RemoteCacheManager second, HotRodServer server) {
         clients.add(first);
         clients.add(second);
         servers.add(server);
      }
   }
}
