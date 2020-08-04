package org.infinispan.server.functional;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.impl.okhttp.StringRestEntityOkHttp;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.test.core.AbstractInfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.junit.After;

/**
 * @author Ryan Emerson
 * @since 12.0
 */
class AbstractMultiClusterIT {

   static final JsonWriter JSON_WRITER = new JsonWriter();

   protected final String config;
   protected Cluster source, target;

   public AbstractMultiClusterIT(String config) {
      this.config = config;
   }

   @After
   public void cleanup() throws Exception {
      stopSourceCluster();
      stopTargetCluster();
   }

   protected void startSourceCluster() {
      source = new Cluster(new ClusterConfiguration(config, 2, 0));
      source.start("source");
   }

   protected void stopSourceCluster() throws Exception {
      if (source != null)
         source.stop("source");
   }

   protected void startTargetCluster() {
      target = new Cluster(new ClusterConfiguration(config, 2, 1000));
      target.start("target");
   }

   protected void stopTargetCluster() throws Exception {
      if (target != null)
         target.stop("target");
   }

   protected int getCacheSize(String cacheName, RestClient restClient) {
      RestCacheClient cache = restClient.cache(cacheName);
      return Integer.parseInt(join(cache.size()).getBody());
   }

   protected void addSchema(RestClient client) {
      RestCacheClient cache = client.cache(PROTOBUF_METADATA_CACHE_NAME);
      RestResponse response = join(cache.put("schema.proto", "message Person {required string name = 1;}"));
      assertEquals(204, response.getStatus());
      RestResponse errorResponse = join(client.cache(PROTOBUF_METADATA_CACHE_NAME).get("schema.proto.errors"));
      assertEquals(404, errorResponse.getStatus());
   }

   protected void createCache(String cacheName, ConfigurationBuilder builder, RestClient client) {
      String cacheConfig = JSON_WRITER.toJSON(builder.build());
      StringRestEntityOkHttp body = new StringRestEntityOkHttp(MediaType.APPLICATION_JSON, cacheConfig);
      RestResponse response = join(client.cache(cacheName).createWithConfiguration(body));
      assertEquals(response.getBody(), 200, response.getStatus());
   }

   protected static class ClusterConfiguration extends InfinispanServerTestConfiguration {
      public ClusterConfiguration(String configurationFile, int numServers, int portOffset) {
         super(configurationFile, numServers, ServerRunMode.EMBEDDED, new Properties(), null, null,
               false, false, false, Collections.emptyList(), null, portOffset);
      }
   }

   /**
    * A simplified embedded cluster not tied to junit
    */
   static class Cluster {
      final AbstractInfinispanServerDriver driver;
      RestClient client;

      Cluster(ClusterConfiguration simpleConfiguration) {
         Properties sysProps = System.getProperties();
         for (String prop : sysProps.stringPropertyNames()) {
            if (prop.startsWith(TestSystemPropertyNames.PREFIX)) {
               simpleConfiguration.properties().put(prop,  sysProps.getProperty(prop));
            }
         }
         this.driver = ServerRunMode.DEFAULT.newDriver(simpleConfiguration);
      }

      void start(String name) {
         driver.prepare(name);
         driver.start(name);
      }

      void stop(String name) throws Exception {
         driver.stop(name);
         if (client != null)
            client.close();
      }

      Set<String> getMembers() {
         String response = join(getClient().cacheManager("default").info()).getBody();
         Json jsonNode = Json.read(response);
         return jsonNode.at("cluster_members").asJsonList().stream().map(Json::asString).collect(Collectors.toSet());
      }

      RestClient getClient() {
         if (client == null) {
            InetSocketAddress serverSocket = driver.getServerSocket(0, 11222);
            client = RestClient.forConfiguration(
                  new RestClientConfigurationBuilder().addServer()
                        .host(serverSocket.getHostName()).port(serverSocket.getPort()).build()
            );
         }
         return client;
      }
   }
}
