package org.infinispan.server.functional;

import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.util.KeyValuePair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @since 11.0
 */
public class RollingUpgradeIT extends AbstractMultiClusterIT {
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
      createSourceClusterCache();

      // Create cache in the target cluster pointing to the source cluster via remote-store
      createTargetClusterCache();

      // Register proto schema
      addSchema(restClientSource);
      addSchema(restClientTarget);

      // Populate source cluster
      populateCluster(restClientSource);

      // Make sure data is accessible from the target cluster
      assertEquals("name-20", getPersonName("20", restClientTarget));

      // Do a rolling upgrade from the target
      doRollingUpgrade(restClientTarget);

      // Do a second rolling upgrade, should be harmless and simply override the data
      doRollingUpgrade(restClientTarget);

      // Disconnect source from the remote store
      disconnectSource(restClientTarget);

      // Stop source cluster
      stopSourceCluster();

      // Assert all nodes are disconnected and data was migrated successfully
      for (int i = 0; i < target.getMembers().size(); i++) {
         RestClient restClient = target.getClient(i);
         assertEquals(ENTRIES, getCacheSize(CACHE_NAME, restClient));
         assertEquals("name-35", getPersonName("35", restClient));
      }
   }

   protected void disconnectSource(RestClient client) {
      assertStatus(NO_CONTENT, client.cache(CACHE_NAME).disconnectSource());
   }

   protected void doRollingUpgrade(RestClient client) {
      assertStatus(OK, client.cache(CACHE_NAME).synchronizeData());
   }

   protected String getPersonName(String id, RestClient client) {
      String body = assertStatus(OK,client.cache(CACHE_NAME).get(id));
      return Json.read(body).at("name").asString();
   }

   public void populateCluster(RestClient client) {
      RestCacheClient cache = client.cache(CACHE_NAME);

      for (int i = 0; i < ENTRIES; i++) {
         String person = createPerson("name-" + i);
         assertStatus(NO_CONTENT, cache.put(String.valueOf(i), person));
      }
      assertEquals(ENTRIES, getCacheSize(CACHE_NAME, client));
   }

   private String createPerson(String name) {
      return String.format("{\"_type\":\"Person\",\"name\":\"%s\"}", name);
   }

   void addRemoteStore(ConfigurationBuilder builder) {
      RemoteStoreConfigurationBuilder storeConfigurationBuilder = builder.clustering()
            .cacheMode(CacheMode.DIST_SYNC).persistence().addStore(RemoteStoreConfigurationBuilder.class);
      storeConfigurationBuilder
            .remoteCacheName(CACHE_NAME)
            .hotRodWrapping(true)
            .protocolVersion(ProtocolVersion.PROTOCOL_VERSION_25)
            .shared(true)
            .addServer()
            .host(source.driver.getServerAddress(0).getHostAddress())
            .port(11222);
      final KeyValuePair<String, String> credentials = getCredentials();
      if (getCredentials() != null) {
         storeConfigurationBuilder.remoteSecurity()
               .authentication().enable().saslMechanism("PLAIN")
               .username(credentials.getKey())
               .password(credentials.getValue())
               .realm("default");
      }
   }

   private void createTargetClusterCache() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      addRemoteStore(builder);

      createCache(CACHE_NAME, builder, target.getClient());
   }

   void createSourceClusterCache() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      createCache(CACHE_NAME, builder, source.getClient());
   }
}
