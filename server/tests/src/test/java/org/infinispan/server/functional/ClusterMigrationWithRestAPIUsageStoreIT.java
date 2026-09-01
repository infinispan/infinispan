package org.infinispan.server.functional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.internal.InternalCacheNames;
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
 * Tests that REST API operations work correctly on caches during rolling upgrade.
 *
 * @since 16.3
 */
public class ClusterMigrationWithRestAPIUsageStoreIT extends AbstractMultiClusterIT {

   protected static final String CACHE_NAME = "migrate";
   protected static final int ENTRIES = 50;
   static final String PROTOBUF_SCHEMAS_INTERNAL_CACHE_NAME = InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME;

   @Override
   protected void startSourceCluster() {
      source = new Cluster(new ClusterConfiguration(config, 1, 0, mavenArtifacts), getCredentials());
      source.start(this.getClass().getName() + "-source");
   }

   @Override
   protected void startTargetCluster() {
      target = new Cluster(new ClusterConfiguration(config, 1, 1000, mavenArtifacts), getCredentials());
      target.start(this.getClass().getName() + "-target");
   }

   @BeforeEach
   public void before() {
      startSourceCluster();
      startTargetCluster();

      assertThat(source.getMembers().size()).isOne();
      assertThat(target.getMembers().size()).isOne();
      assertThat(source.getMembers()).isNotSameAs(target.getMembers());
   }

   @AfterEach
   public void after() throws Exception {
      stopTargetCluster();
      stopSourceCluster();
   }

   @Test
   public void testMigrationKeepsValueFormat() throws Exception {
      RestClient restClientSource = source.getClient();
      RestClient restClientTarget = target.getClient();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      createCache(CACHE_NAME, builder, restClientSource);
      createCache(CACHE_NAME, builder, restClientTarget);

      // Put simple key/value on the source
      RestCacheClient sourceCache = restClientSource.cache(CACHE_NAME);
      assertStatus(NO_CONTENT, sourceCache.put("1", "1"));

      // Read the value from the source
      String sourceValue = assertStatus(OK, sourceCache.get("1"));

      // Connect target to source and migrate
      connectTargetCluster(CACHE_NAME);
      assertSourceConnected(CACHE_NAME);

      // Read through the remote store before migration — value should match the source
      String targetValueBeforeMigration = assertStatus(OK, restClientTarget.cache(CACHE_NAME).get("1"));
      assertEquals(sourceValue, targetValueBeforeMigration);

      migrate(CACHE_NAME, restClientTarget);
      disconnectSource(CACHE_NAME, restClientTarget);
      assertSourceDisconnected(CACHE_NAME);

      // After migration — value should still match the source
      String targetValueAfterMigration = assertStatus(OK, restClientTarget.cache(CACHE_NAME).get("1"));
      assertEquals(sourceValue, targetValueAfterMigration);
   }

   @Test
   public void testStreamEntriesDuringRollingUpgrade() throws Exception {
      RestClient restClientSource = source.getClient();
      RestClient restClientTarget = target.getClient();

      addSchema(restClientSource);

      createSourceClusterCache(CACHE_NAME);
      populateCache(CACHE_NAME, restClientSource);

      // Migrate schemas to the target so the transcoder can handle protostream entries
      connectTargetCluster(PROTOBUF_SCHEMAS_INTERNAL_CACHE_NAME);
      migrate(PROTOBUF_SCHEMAS_INTERNAL_CACHE_NAME, restClientTarget);
      disconnectSource(PROTOBUF_SCHEMAS_INTERNAL_CACHE_NAME, restClientTarget);

      createCache(CACHE_NAME, indexedCacheBuilder(), target.getClient());

      // Connect the target to the source cluster (adds a RemoteStore dynamically)
      connectTargetCluster(CACHE_NAME);
      assertSourceConnected(CACHE_NAME);

      // Stream entries from the target cache while the remote store is active
      try (RestResponse response = sync(restClientTarget.cache(CACHE_NAME).entries())) {
         assertEquals(OK, response.status());
         Collection<?> entries = (Collection<?>) Json.read(response.body()).getValue();
         assertEquals(ENTRIES, entries.size());
      }

      // Migrate and disconnect
      migrate(CACHE_NAME, restClientTarget);
      disconnectSource(CACHE_NAME, restClientTarget);
      assertSourceDisconnected(CACHE_NAME);

      // Stream entries after migration completes
      try (RestResponse response = sync(restClientTarget.cache(CACHE_NAME).entries())) {
         assertEquals(OK, response.status());
         Collection<?> entries = (Collection<?>) Json.read(response.body()).getValue();
         assertEquals(ENTRIES, entries.size());
      }
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

   protected void migrate(String cacheName, RestClient client) {
      assertStatus(OK, client.cache(cacheName).synchronizeData());
   }

   public void populateCache(String cacheName, RestClient client) {
      RestCacheClient cache = client.cache(cacheName);
      for (int i = 0; i < ENTRIES; i++) {
         String person = String.format("{\"_type\":\"Person\",\"name\":\"name-%d\"}", i);
         assertStatus(NO_CONTENT, cache.put(String.valueOf(i), person));
      }
      assertEquals(ENTRIES, getCacheSize(cacheName, client));
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
