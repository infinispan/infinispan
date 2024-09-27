package org.infinispan.client.hotrod.hash;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.CodecHolder;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelOperation;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.server.hotrod.HotRodServer;
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

      for (int i = 0; i < 100; i++) {
         Object keyValue = kv(i);
         ((ControlledChannelFactory) rcm.getChannelFactory()).useOnFetch((server, op) -> {
            requests.put(keyValue, server);
         });
         cache.put(keyValue, keyValue);
      }

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
      return new InternalRemoteCacheManager(cfg, new ControlledChannelFactory(cfg));
   }

   private static class ControlledChannelFactory extends ChannelFactory {

      private BiConsumer<SocketAddress, ChannelOperation> onFetch;

      public ControlledChannelFactory(Configuration cfg) {
         super(cfg, new CodecHolder(cfg.version().getCodec()));
      }

      public void useOnFetch(BiConsumer<SocketAddress, ChannelOperation> onFetch) {
         this.onFetch = onFetch;
      }

      @Override
      public <T extends ChannelOperation> T fetchChannelAndInvoke(SocketAddress server, T operation) {
         if (onFetch != null) onFetch.accept(server, operation);
         return super.fetchChannelAndInvoke(server, operation);
      }
   }
}
