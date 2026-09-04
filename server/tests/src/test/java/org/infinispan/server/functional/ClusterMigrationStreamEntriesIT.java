package org.infinispan.server.functional;

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
 * Reproduces NPE in EncoderEntryMapper when streaming entries from a cache
 * with a dynamically added RemoteStore (rolling upgrade scenario).
 *
 * @since 16.3
 */
public class ClusterMigrationStreamEntriesIT extends AbstractMultiClusterIT {

   protected static final String CACHE_NAME = "stream-test";
   protected static final int ENTRIES = 10;

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
   }

   @AfterEach
   public void after() throws Exception {
      stopTargetCluster();
      stopSourceCluster();
   }

   @Test
   public void testStreamEntriesWithRemoteStore() throws Exception {
      RestClient restClientSource = source.getClient();
      RestClient restClientTarget = target.getClient();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      createCache(CACHE_NAME, builder, restClientSource);
      createCache(CACHE_NAME, builder, restClientTarget);

      RestCacheClient sourceCache = restClientSource.cache(CACHE_NAME);
      for (int i = 0; i < ENTRIES; i++) {
         assertStatus(NO_CONTENT, sourceCache.put(String.valueOf(i), "value-" + i));
      }
      assertEquals(ENTRIES, getCacheSize(CACHE_NAME, restClientSource));

      connectTargetCluster(CACHE_NAME);
      assertSourceConnected(CACHE_NAME);

      // Stream entries from the target while RemoteStore is active.
      // Before the fix, this throws NPE in EncoderEntryMapper because
      // entries from RemoteStore have null metadata.
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
      if (credentials != null) {
         storeConfigurationBuilder.remoteSecurity()
               .authentication().enable().saslMechanism("PLAIN")
               .username(credentials.getKey())
               .password(credentials.getValue())
               .realm("default");
      }
   }
}
