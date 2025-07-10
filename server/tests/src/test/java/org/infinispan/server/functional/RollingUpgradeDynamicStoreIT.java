package org.infinispan.server.functional;

import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.io.IOException;
import java.util.List;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.RemoteServerConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.persistence.remote.upgrade.SerializationUtils;
import org.infinispan.util.KeyValuePair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @since 13.0
 */
public class RollingUpgradeDynamicStoreIT extends AbstractMultiClusterIT {

   protected static final String CACHE_NAME = "rolling";
   protected static final int ENTRIES = 50;

   @BeforeEach
   public void before() {
      // Start two embedded clusters with 2-node each
      startSourceCluster();
      startTargetCluster();

      // Assert clusters are isolated and have 2 members each
      assertEquals(2, source.getMembers().size());
      assertEquals(2, target.getMembers().size());
      assertNotSame(source.getMembers(), target.getMembers());
   }

   @AfterEach
   public void after() throws Exception {
      stopTargetCluster();
      stopSourceCluster();
   }

   @Test
   public void testRollingUpgrade() throws Exception {
      RestClient restClientSource = source.getClient();
      RestClient restClientTarget = target.getClient();

      // Create cache in the source cluster
      createSourceClusterCache(CACHE_NAME);

      // Create cache in the target cluster identical to the source, without any store
      createTargetClusterWithoutStore();

      // Register proto schema
      addSchema(restClientSource);

      // Populate source cluster
      populateCache(CACHE_NAME, restClientSource);

      // Connect target cluster to the source cluster
      assertSourceDisconnected(PROTOBUF_METADATA_CACHE_NAME);
      assertSourceDisconnected(CACHE_NAME);
      connectTargetCluster(PROTOBUF_METADATA_CACHE_NAME);
      connectTargetCluster(CACHE_NAME);
      assertSourceConnected(PROTOBUF_METADATA_CACHE_NAME);
      assertSourceConnected(CACHE_NAME);

      // Make sure data is accessible from the target cluster
      assertStatus(NO_CONTENT, restClientTarget.cache(PROTOBUF_METADATA_CACHE_NAME).exists());
      assertEquals("name-13", getPersonName("13", restClientTarget));

      // Do a rolling upgrade from the target
      doRollingUpgrade(PROTOBUF_METADATA_CACHE_NAME, restClientTarget);
      doRollingUpgrade(CACHE_NAME, restClientTarget);

      // Do a second rolling upgrade, should be harmless and simply override the data
      doRollingUpgrade(PROTOBUF_METADATA_CACHE_NAME, restClientTarget);
      doRollingUpgrade(CACHE_NAME, restClientTarget);

      // Disconnect source from the remote store
      disconnectSource(PROTOBUF_METADATA_CACHE_NAME, restClientTarget);
      disconnectSource(CACHE_NAME, restClientTarget);
      assertSourceDisconnected(PROTOBUF_METADATA_CACHE_NAME);
      assertSourceDisconnected(CACHE_NAME);

      // Stop source cluster
      stopSourceCluster();

      // Assert all nodes are disconnected and data was migrated successfully
      for (int i = 0; i < target.getMembers().size(); i++) {
         RestClient restClient = target.getClient(i);
         assertEquals(ENTRIES, getCacheSize(CACHE_NAME, restClient));
         assertEquals("name-35", getPersonName("35", restClient));
         assertStatus(NO_CONTENT, restClient.cache(PROTOBUF_METADATA_CACHE_NAME).exists());
      }
   }

   private void createTargetClusterWithoutStore() {
      createCache(CACHE_NAME, indexedCacheBuilder(), target.getClient());
   }

   protected void connectTargetCluster(String cacheName) throws IOException {
      RestCacheClient client = target.getClient().cache(cacheName);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      addRemoteStore(cacheName, builder);

      RemoteStoreConfiguration remoteStore = (RemoteStoreConfiguration) builder.build().persistence().stores().iterator().next();

      RestEntity restEntity = RestEntity.create(MediaType.APPLICATION_JSON, SerializationUtils.toJson(remoteStore));
      assertStatus(NO_CONTENT, client.connectSource(restEntity));

      String json = assertStatus(OK, client.sourceConnection());
      RemoteStoreConfiguration remoteStoreConfiguration = SerializationUtils.fromJson(json);

      List<RemoteServerConfiguration> servers = remoteStoreConfiguration.servers();
      assertEquals(1, servers.size());
      RemoteServerConfiguration initialConfig = remoteStore.servers().iterator().next();
      assertEquals(initialConfig.host(), servers.get(0).host());
      assertEquals(initialConfig.port(), servers.get(0).port());
   }

   protected void assertSourceConnected(String cacheName) {
      assertStatus(OK, target.getClient().cache(cacheName).sourceConnected());
   }

   protected void assertSourceDisconnected(String cacheName) {
      assertStatus(NOT_FOUND, target.getClient().cache(cacheName).sourceConnected());
   }

   protected void disconnectSource(String cacheName, RestClient client) {
      assertStatus(NO_CONTENT, client.cache(cacheName).disconnectSource());
   }

   protected void doRollingUpgrade(String cacheName, RestClient client) {
      assertStatus(OK, client.cache(cacheName).synchronizeData());
   }

   protected String getPersonName(String id, RestClient client) {
      String body = assertStatus(OK,client.cache(CACHE_NAME).get(id));
      Json value = Json.read(Json.read(body).at("_value").asString());
      return value.at("name").asString();
   }

   public void populateCache(String cacheName, RestClient client) {
      RestCacheClient cache = client.cache(cacheName);

      for (int i = 0; i < ENTRIES; i++) {
         String person = createPerson("name-" + i);
         assertStatus(NO_CONTENT, cache.put(String.valueOf(i), person));
      }
      assertEquals(ENTRIES, getCacheSize(cacheName, client));
   }

   private String createPerson(String name) {
      return String.format("{\"_type\":\"Person\",\"name\":\"%s\"}", name);
   }

   void addRemoteStore(String cacheName, ConfigurationBuilder builder) {
      RemoteStoreConfigurationBuilder storeConfigurationBuilder = builder.clustering()
            .cacheMode(CacheMode.DIST_SYNC).persistence().addStore(RemoteStoreConfigurationBuilder.class);
      storeConfigurationBuilder
            .remoteCacheName(cacheName)
            .segmented(false)
            .shared(true)
            .addServer()
            .host(source.driver.getServerAddress(0).getHostAddress())
            .port(11222)
            .addProperty(RemoteStore.MIGRATION, "true");
      final KeyValuePair<String, String> credentials = getCredentials();
      if (getCredentials() != null) {
         storeConfigurationBuilder.remoteSecurity()
               .authentication().enable().saslMechanism("PLAIN")
               .username(credentials.getKey())
               .password(credentials.getValue())
               .realm("default");
      }
   }

   void createSourceClusterCache(String cacheName) {
      createCache(cacheName, indexedCacheBuilder(), source.getClient());
   }

   ConfigurationBuilder indexedCacheBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.indexing().enable().addIndexedEntities("Person");
      return builder;
   }
}
