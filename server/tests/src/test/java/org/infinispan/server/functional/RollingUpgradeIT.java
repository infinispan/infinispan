package org.infinispan.server.functional;

import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @since 11.0
 */
public class RollingUpgradeIT extends AbstractMultiClusterIT {
   protected static final String CACHE_NAME = "rolling";
   private static final int ENTRIES = 50;

   public RollingUpgradeIT() {
      super("configuration/ClusteredServerTest.xml");
   }

   @Before
   public void before() {
      // Start two embedded clusters with 2-node each
      startSourceCluster();
      startTargetCluster();

      // Assert clusters are isolated and have 2 members each
      assertEquals(2, source.getMembers().size());
      assertEquals(2, target.getMembers().size());
      assertNotSame(source.getMembers(), target.getMembers());
   }

   @After
   public void after() throws Exception {
      source.stop("source");
      target.stop("target");
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
      RestResponse response = join(client.cache(CACHE_NAME).disconnectSource());
      assertEquals(204, response.getStatus());
   }

   protected void doRollingUpgrade(RestClient client) {
      RestResponse response = join(client.cache(CACHE_NAME).synchronizeData());
      assertEquals(response.getBody(), 200, response.getStatus());
   }

   private String getPersonName(String id, RestClient client) {
      RestResponse resp = join(client.cache(CACHE_NAME).get(id));
      String body = resp.getBody();
      assertEquals(body, 200, resp.getStatus());
      return Json.read(body).at("name").asString();
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

   private void createTargetClusterCache() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).persistence()
            .addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName(CACHE_NAME)
            .hotRodWrapping(true)
            .protocolVersion(ProtocolVersion.PROTOCOL_VERSION_25)
            .shared(true)
            .addServer()
            .host(source.driver.getServerAddress(0).getHostAddress())
            .port(11222);

      createCache(CACHE_NAME, builder, target.getClient());
   }

   private void createSourceClusterCache() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      createCache(CACHE_NAME, builder, source.getClient());
   }
}
