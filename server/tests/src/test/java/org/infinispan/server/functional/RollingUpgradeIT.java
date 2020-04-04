package org.infinispan.server.functional;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.impl.okhttp.StringRestEntityOkHttp;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.server.test.core.EmbeddedInfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.ServerRunMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @since 11.0
 */
public class RollingUpgradeIT {
   private static final String CACHE_NAME = "rolling";
   private static final int ENTRIES = 50;
   public static final JsonWriter JSON_WRITER = new JsonWriter();
   public static final ObjectMapper MAPPER = new ObjectMapper();

   private Cluster source, target;
   private RestClient restClientSource, restClientTarget;

   @Before
   public void before() {
      String path = getClass().getClassLoader().getResource("configuration/ClusteredServerTest.xml").getPath();
      // Start two embedded clusters with 2-node each
      source = new Cluster(new ClusterConfiguration(path, 2));
      target = new Cluster(new ClusterConfiguration(path, 2), 1000);
      source.start("source");
      target.start("target");

      // Assert clusters are isolated and have 2 members each
      assertEquals(2, source.getMembers().size());
      assertEquals(2, target.getMembers().size());
      assertNotSame(source.getMembers(), target.getMembers());

      restClientSource = source.getClient(0);
      restClientTarget = target.getClient(1);
   }

   @After
   public void after() {
      source.stop("source");
      target.stop("target");
   }

   @Test
   public void testRollingUpgrade() throws IOException {
      // Create cache in the source cluster
      createSourceClusterCache(restClientSource);

      // Create cache in the target cluster pointing to the source cluster via remote-store
      createTargetClusterCache(restClientTarget, 11222);

      // Register proto schema
      addSchema(restClientSource);
      addSchema(restClientTarget);

      // Populate source cluster
      populateCluster(restClientSource);

      // Make sure data is accessible from the target cluster
      assertEquals("name-20", getPersonName("20", restClientTarget));

      // Do a rolling upgrade from the target
      doRollingUpgrade(restClientTarget);

      // Disconnect source from the remote store
      disconnectSource(restClientTarget);

      // Assert data was migrated successfully
      assertEquals(ENTRIES, getCacheSize(CACHE_NAME, restClientTarget));
   }

   private int getCacheSize(String cacheName, RestClient restClient) {
      RestCacheClient cache = restClient.cache(cacheName);
      return Integer.parseInt(join(cache.size()).getBody());
   }

   private void disconnectSource(RestClient client) {
      RestResponse response = join(client.cache(CACHE_NAME).disconnectSource());
      assertEquals(200, response.getStatus());
   }

   private void doRollingUpgrade(RestClient client) {
      RestResponse response = join(client.cache(CACHE_NAME).synchronizeData());
      assertEquals(response.getBody(), 200, response.getStatus());
   }

   private String getPersonName(String id, RestClient client) throws IOException {
      RestResponse resp = join(client.cache(CACHE_NAME).get(id));
      String body = resp.getBody();
      assertEquals(body, 200, resp.getStatus());
      return MAPPER.readTree(body).get("name").asText();
   }

   public void populateCluster(RestClient client) {
      RestCacheClient cache = client.cache(CACHE_NAME);

      for (int i = 0; i < ENTRIES; i++) {
         String person = createPerson("name-" + i);
         join(cache.put(String.valueOf(i), person));
      }
      assertEquals(ENTRIES, getCacheSize(CACHE_NAME, client));
   }

   private String createPerson(String name) {
      return String.format("{\"_type\":\"Person\",\"name\":\"%s\"}", name);
   }

   private void addSchema(RestClient client) {
      RestCacheClient cache = client.cache(PROTOBUF_METADATA_CACHE_NAME);
      RestResponse response = join(cache.put("schema.proto", "message Person {required string name = 1;}"));
      assertEquals(204, response.getStatus());
      RestResponse errorResponse = join(client.cache(PROTOBUF_METADATA_CACHE_NAME).get("schema.proto.errors"));
      assertEquals(404, errorResponse.getStatus());
   }

   private void createTargetClusterCache(RestClient client, int port) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).persistence()
            .addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName(CACHE_NAME)
            .hotRodWrapping(true)
            .protocolVersion(ProtocolVersion.PROTOCOL_VERSION_25)
            .addServer().host("127.0.0.1")
            .port(port);

      String cacheConfig = JSON_WRITER.toJSON(builder.build());
      StringRestEntityOkHttp body = new StringRestEntityOkHttp(MediaType.APPLICATION_JSON, cacheConfig);
      RestResponse response = join(client.cache(CACHE_NAME).createWithConfiguration(body));
      assertEquals(response.getBody(), 200, response.getStatus());
   }

   private void createSourceClusterCache(RestClient client) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      String cacheConfig = JSON_WRITER.toJSON(builder.build());
      RestEntity body = new StringRestEntityOkHttp(MediaType.APPLICATION_JSON, cacheConfig);
      RestResponse response = join(client.cache(CACHE_NAME).createWithConfiguration(body));
      assertEquals(200, response.getStatus());
   }

   private static class ClusterConfiguration extends InfinispanServerTestConfiguration {
      public ClusterConfiguration(String configurationFile, int numServers) {
         super(configurationFile, numServers, ServerRunMode.EMBEDDED, new Properties(), null, null,
               false, false, false, Collections.emptyList());
      }
   }

   /**
    * A simplified embedded cluster not tied to junit
    */
   static class Cluster extends EmbeddedInfinispanServerDriver {
      private final int clusterPortOffset;

      public Cluster(ClusterConfiguration simpleConfiguration, int clusterPortOffset) {
         super(simpleConfiguration);
         this.clusterPortOffset = clusterPortOffset;
      }

      @Override
      public void start(String name) {
         super.prepare(name);
         super.start(name);
      }

      public Cluster(ClusterConfiguration simpleConfiguration) {
         this(simpleConfiguration, 0);
      }

      @Override
      protected int clusterPortOffset() {
         return clusterPortOffset;
      }


      protected Set<String> getMembers() {
         String response = join(getClient(0).cacheManager("default").info()).getBody();
         try {
            JsonNode jsonNode = MAPPER.readTree(response);
            Set<String> names = new HashSet<>();
            jsonNode.get("cluster_members").getElements().forEachRemaining(n -> names.add(n.asText()));
            return names;
         } catch (IOException e) {
            Assert.fail(e.getMessage());
         }
         return null;
      }

      protected RestClient getClient(int server) {
         InetSocketAddress serverSocket = getServerSocket(server, 11222);
         return RestClient.forConfiguration(
               new RestClientConfigurationBuilder().addServer()
                     .host("localhost").port(serverSocket.getPort()).build()
         );
      }
   }
}
