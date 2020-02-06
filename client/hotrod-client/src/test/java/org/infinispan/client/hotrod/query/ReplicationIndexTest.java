package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.CacheMode.REPL_SYNC;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.test.TestingUtil.blockUntilCacheStatusAchieved;
import static org.infinispan.test.TestingUtil.blockUntilViewReceived;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Test indexing during state transfer.
 */
@Test(groups = "functional", testName = "client.hotrod.query.ReplicationIndexTest")
public class ReplicationIndexTest extends MultiHotRodServersTest {

   public static final String CACHE_NAME = "test-cache";
   public static final String PROTO_FILE = "file.proto";
   public static final int ENTRIES = 2;

   private final AtomicInteger serverCount = new AtomicInteger(0);

   protected void addNode() throws IOException {
      int index = serverCount.incrementAndGet();

      // Add a new Hot Rod server
      addHotRodServer(getDefaultClusteredCacheConfig(REPL_SYNC));
      EmbeddedCacheManager cacheManager = manager(index - 1);

      // Add the test caches
      org.infinispan.configuration.cache.ConfigurationBuilder builder = getDefaultClusteredCacheConfig(REPL_SYNC, isTransactional());
      builder.indexing().enable().addProperty("default.directory_provider", "local-heap");
      cacheManager.defineConfiguration(CACHE_NAME, builder.build());

      // Wait for state transfer on the test caches
      Cache<?, ?> cache = cacheManager.getCache(CACHE_NAME);
      blockUntilViewReceived(cache, index);
      blockUntilCacheStatusAchieved(cache, ComponentStatus.RUNNING, 10000);
      Collection<Cache<?, ?>> caches = cacheManagers.stream().map(cm -> cm.getCache(CACHE_NAME)).collect(Collectors.toList());
      TestingUtil.waitForNoRebalance(caches);

      // Client a client that goes exclusively to the Hot Rod server
      RemoteCacheManager remoteCacheManager = createClient(index - 1);
      clients.add(remoteCacheManager);

      // Create client and server Serialization Contexts
      SerializationContext serCtx = MarshallerUtil.getSerializationContext(remoteCacheManager);
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String protoFile = protoSchemaBuilder.fileName(PROTO_FILE).addClass(Entity.class).build(serCtx);

      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put(PROTO_FILE, protoFile);
      assertFalse(metadataCache.containsKey(ERRORS_KEY_SUFFIX));
   }

   protected boolean isTransactional() {
      return false;
   }

   protected RemoteCacheManager createClient(int i) {
      HotRodServer server = server(i);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer()
            .host(server.getHost())
            .port(server.getPort())
            .marshaller(new ProtoStreamMarshaller())
            .balancingStrategy(() -> new FixedServerBalancing(server));
      return new InternalRemoteCacheManager(clientBuilder.build());
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      addNode();
   }

   @ProtoDoc("@Indexed")
   static class Entity {
      private String name;

      public Entity() {
      }

      static Entity create(String name) {
         Entity entity = new Entity();
         entity.setName(name);
         return entity;
      }

      @ProtoDoc("@Field(index=Index.YES, analyze = Analyze.YES, store = Store.NO)")
      @ProtoField(number = 1, required = true)
      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

   }

   static class FixedServerBalancing implements FailoverRequestBalancingStrategy {
      private final HotRodServer server;

      public FixedServerBalancing(HotRodServer server) {
         this.server = server;
      }

      @Override
      public void setServers(Collection<SocketAddress> servers1) {
      }

      @Override
      public SocketAddress nextServer(Set<SocketAddress> failedServers) {
         return InetSocketAddress.createUnresolved(server.getHost(), server.getPort());
      }
   }

   private int queryCount(String query, RemoteCache<?, ?> remoteCache) {
      return Search.getQueryFactory(remoteCache).create(query).getResultSize();
   }

   @Test
   public void testIndexingDuringStateTransfer() throws IOException {
      RemoteCache<Object, Object> remoteCache = clients.get(0).getCache(CACHE_NAME);

      for (int i = 0; i < ENTRIES; i++) {
         remoteCache.put(i, Entity.create("name" + i));
      }

      assertIndexed(remoteCache);

      addNode();

      RemoteCache<Object, Object> secondRemoteCache = clients.get(1).getCache(CACHE_NAME);
      assertIndexed(secondRemoteCache);
   }

   private void assertIndexed(RemoteCache<?, ?> remoteCache) {
      assertEquals(ENTRIES, remoteCache.size());
      assertEquals(ENTRIES, queryCount("FROM Entity", remoteCache));
      assertEquals(1, queryCount("FROM Entity where name:'name1'", remoteCache));
   }

}
