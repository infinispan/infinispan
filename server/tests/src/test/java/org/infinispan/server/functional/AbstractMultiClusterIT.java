package org.infinispan.server.functional;

import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.assertStatusAndBodyContains;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.ServerConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.test.core.AbstractInfinispanServerDriver;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.util.KeyValuePair;
import org.junit.jupiter.api.AfterEach;

/**
 * @author Ryan Emerson
 * @since 12.0
 */
abstract class AbstractMultiClusterIT {

   protected final String config;
   protected final String[] mavenArtifacts;
   protected Cluster source, target;

   public AbstractMultiClusterIT(String... mavenArtifacts) {
      this.config = configFile();
      this.mavenArtifacts = mavenArtifacts;
   }

   protected String configFile() {
      return "configuration/ClusteredServerTest.xml";
   }

   @AfterEach
   public void cleanup() throws Exception {
      stopSourceCluster();
      stopTargetCluster();
   }

   protected void startSourceCluster() {
      source = new Cluster(new ClusterConfiguration(config, 2, 0, mavenArtifacts), getCredentials());
      source.start(this.getClass().getName() + "-source");
   }

   protected void stopSourceCluster() throws Exception {
      if (source != null)
         source.stop(this.getClass().getName() + "-source");
   }

   protected void startTargetCluster() {
      target = new Cluster(new ClusterConfiguration(config, 2, 1000, mavenArtifacts), getCredentials());
      target.start(this.getClass().getName() + "-target");
   }

   protected void stopTargetCluster() throws Exception {
      if (target != null)
         target.stop(this.getClass().getName() + "-target");
   }

   protected int getCacheSize(String cacheName, RestClient restClient) {
      RestCacheClient cache = restClient.cache(cacheName);
      return Integer.parseInt(assertStatus(OK, cache.size()));
   }

   protected void addSchema(RestClient client) {
      assertStatus(OK, client.schemas().put("schema.proto", "/* @Indexed */ message Person {  /* @Text(projectable = true) */ required string name = 1; }"));
      assertStatusAndBodyContains(OK, "schema.proto", client.schemas().names());
   }

   protected void createCache(String cacheName, ConfigurationBuilder builder, RestClient client) {
      String cacheConfig = Common.cacheConfigToJson(cacheName, builder.build());
      RestEntity entity = RestEntity.create(MediaType.APPLICATION_JSON, cacheConfig);
      assertStatus(OK, client.cache(cacheName).createWithConfiguration(entity));
   }

   protected KeyValuePair<String, String> getCredentials() {
      return null;
   }

   protected static class ClusterConfiguration extends InfinispanServerTestConfiguration {
      public ClusterConfiguration(String configurationFile, int numServers, int portOffset, String[] mavenArtifacts) {
         super(configurationFile, numServers, numServers, mavenArtifacts != null ? ServerRunMode.CONTAINER : ServerRunMode.EMBEDDED, new Properties(), mavenArtifacts, null,
               false, false, false, Collections.emptyList(), null, null, portOffset, Util.EMPTY_STRING_ARRAY, Util.EMPTY_STRING_ARRAY);
      }
   }

   /**
    * A simplified embedded cluster not tied to junit
    */
   static class Cluster {
      final AbstractInfinispanServerDriver driver;
      final Map<Integer, RestClient> serverClients = new HashMap<>();
      private final KeyValuePair<String, String> credentials;

      Cluster(ClusterConfiguration simpleConfiguration) {
         this(simpleConfiguration, null);
      }

      Cluster(ClusterConfiguration simpleConfiguration, KeyValuePair<String, String> credentials) {
         this.credentials = credentials;
         Properties sysProps = System.getProperties();
         for (String prop : sysProps.stringPropertyNames()) {
            if (prop.startsWith(TestSystemPropertyNames.PREFIX)) {
               simpleConfiguration.properties().put(prop, sysProps.getProperty(prop));
            }
         }
         this.driver = simpleConfiguration.runMode().newDriver(simpleConfiguration);
      }

      void start(String name) {
         driver.prepare(name);
         driver.start(name);
      }

      void stop(String name) throws Exception {
         driver.stop(name);
         for (RestClient client : serverClients.values())
            client.close();
      }

      Set<String> getMembers() {
         String response = assertStatus(OK, getClient().container().info());
         Json jsonNode = Json.read(response);
         return jsonNode.at("cluster_members").asJsonList().stream().map(Json::asString).collect(Collectors.toSet());
      }

      int getSinglePort(int server) {
         return driver.getServerSocket(server, 11222).getPort();
      }

      RestClient getClient() {
         return getClient(0);
      }
      RestClient getClient(int server) {
         return serverClients.computeIfAbsent(server, k -> {
            InetSocketAddress serverSocket = driver.getServerSocket(server, 11222);
            final ServerConfigurationBuilder configurationBuilder = new RestClientConfigurationBuilder().addServer()
                  .host(serverSocket.getHostString()).port(serverSocket.getPort());
            if (credentials != null) {
               String user = credentials.getKey();
               String pass = credentials.getValue();
               configurationBuilder.security().authentication().enable().mechanism("BASIC").username(user).password(pass);
            }
            return RestClient.forConfiguration(configurationBuilder.build());
         });
      }
   }
}
