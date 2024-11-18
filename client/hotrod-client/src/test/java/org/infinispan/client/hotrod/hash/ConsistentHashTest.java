package org.infinispan.client.hotrod.hash;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.hash.ConsistentHashTest")
public class ConsistentHashTest extends MultiHotRodServersTest {

   private static final int NUM_SERVERS = 3;

   private MediaType keyType = null;

   protected ConsistentHashTest withKeyType(MediaType keyType) {
      this.keyType = keyType;
      return this;
   }

   public void testKeysMapToCorrectSegment() {
      RemoteCacheManager rcm = clients.get(0);
      rcm.start();

      Map<Object, SocketAddress> requests = new HashMap<>();
      RemoteCache<Object, Object> cache = rcm.getCache();

      CacheOperationsFactory cof = TestingUtil.extractField(cache, "operationsFactory");
      OperationDispatcher dispatcher = TestingUtil.extractField(rcm, "dispatcher");

      Answer<?> answer = AdditionalAnswers.delegatesTo(cof);
      CacheOperationsFactory mock = Mockito.mock(CacheOperationsFactory.class, answer);

      // Collect the routing object used for each operation and then determine its address to confirm it
      // was routed to the proper owner
      Mockito.doAnswer(i -> {
         HotRodOperation<?> op = (HotRodOperation<?>) answer.answer(i);
         SocketAddress socketAddress = dispatcher.addressForObject(op.getRoutingObject(), cache.getName());
         assertThat(socketAddress).isNotNull();

         requests.put(i.getArguments()[0], socketAddress);
         return op;
      }).when(mock).newPutKeyValueOperation(Mockito.any(), Mockito.any(), Mockito.anyLong(),
            Mockito.any(TimeUnit.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));

      TestingUtil.replaceField(mock, "operationsFactory", cache, RemoteCacheImpl.class);

      for (int i = 0; i < 100; i++) {
         Object keyValue = kv(i);

         cache.put(keyValue, keyValue);
      }

      assertThat(requests).hasSize(100);

      for (Map.Entry<Object, SocketAddress> entry : requests.entrySet()) {
         int port = ((InetSocketAddress) entry.getValue()).getPort();
         HotRodServer server = findServer(port);

         Cache<Object, Object> c = server.getCacheManager()
                     .getCache();

         Object r = c.getAdvancedCache()
               .withFlags(Flag.CACHE_MODE_LOCAL)
               .get(entry.getKey());

         assertThat(r).isEqualTo(entry.getKey());
      }
   }

   private Object kv(int i) {
      return String.valueOf(i);
   }

   private HotRodServer findServer(int port) {
      for (HotRodServer server : servers) {
         if (server.getAddress().getPort() == port)
            return server;
      }
      throw new IllegalStateException("Server not found for port: " + port);
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new ConsistentHashTest(),
            new ConsistentHashTest().withKeyType(MediaType.APPLICATION_PROTOSTREAM),
            new ConsistentHashTest().withKeyType(MediaType.APPLICATION_OBJECT),
            new ConsistentHashTest().withKeyType(MediaType.TEXT_PLAIN),
      };
   }

   @Override
   protected String parameters() {
      return " -- key-type=" + keyType;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      if (keyType != null) builder.encoding().key().mediaType(keyType);

      // We want to ensure only a single copy exist and the client is sending to the correct owner.
      builder.clustering().hash().numOwners(1);

      return builder;
   }

   @Override
   protected RemoteCacheManager createClient(int i) {
      Configuration cfg = createHotRodClientConfigurationBuilder(server(i)).build();
      return new RemoteCacheManager(cfg);
   }
}
