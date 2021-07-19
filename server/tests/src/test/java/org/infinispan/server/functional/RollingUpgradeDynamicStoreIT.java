package org.infinispan.server.functional;

import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.remote.configuration.RemoteServerConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.upgrade.SerializationUtils;
import org.junit.Test;

/**
 * @since 13.0
 */
public class RollingUpgradeDynamicStoreIT extends RollingUpgradeIT {
   public RollingUpgradeDynamicStoreIT() {
      super("configuration/ClusteredServerTest.xml");
   }

   @Test
   @Override
   public void testRollingUpgrade() throws Exception {
      RestClient restClientSource = source.getClient();
      RestClient restClientTarget = target.getClient();

      // Create cache in the source cluster
      createSourceClusterCache();

      // Create cache in the target cluster identical to the source, without any store
      createTargetClusterWithoutStore();

      // Register proto schema
      addSchema(restClientSource);
      addSchema(restClientTarget);

      // Populate source cluster
      populateCluster(restClientSource);

      // Connect target cluster to the source cluster
      assertSourceDisconnected();
      connectTargetCluster();
      assertSourceConnected();

      // Make sure data is accessible from the target cluster
      assertEquals("name-13", getPersonName("13", restClientTarget));

      // Do a rolling upgrade from the target
      doRollingUpgrade(restClientTarget);

      // Do a second rolling upgrade, should be harmless and simply override the data
      doRollingUpgrade(restClientTarget);

      // Disconnect source from the remote store
      disconnectSource(restClientTarget);
      assertSourceDisconnected();

      // Stop source cluster
      stopSourceCluster();

      // Assert all nodes are disconnected and data was migrated successfully
      for (int i = 0; i < target.getMembers().size(); i++) {
         RestClient restClient = target.getClient(i);
         assertEquals(ENTRIES, getCacheSize(CACHE_NAME, restClient));
         assertEquals("name-35", getPersonName("35", restClient));
      }
   }

   private void createTargetClusterWithoutStore() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      createCache(CACHE_NAME, builder, target.getClient());
   }

   protected void connectTargetCluster() throws IOException {
      RestCacheClient client = target.getClient().cache(CACHE_NAME);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      addRemoteStore(builder);

      RemoteStoreConfiguration remoteStore = (RemoteStoreConfiguration) builder.build().persistence().stores().iterator().next();

      RestEntity restEntity = RestEntity.create(MediaType.APPLICATION_JSON, SerializationUtils.toJson(remoteStore));
      RestResponse response = join(client.connectSource(restEntity));
      assertEquals(response.getBody(), 204, response.getStatus());

      response = join(client.sourceConnection());
      RemoteStoreConfiguration remoteStoreConfiguration = SerializationUtils.fromJson(response.getBody());

      List<RemoteServerConfiguration> servers = remoteStoreConfiguration.servers();
      assertEquals(1, servers.size());
      RemoteServerConfiguration initialConfig = remoteStore.servers().iterator().next();
      assertEquals(initialConfig.host(), servers.get(0).host());
      assertEquals(initialConfig.port(), servers.get(0).port());
   }

   protected void assertSourceConnected() {
      RestResponse response = join(target.getClient().cache(CACHE_NAME).sourceConnected());
      assertEquals(200, response.getStatus());
   }

   protected void assertSourceDisconnected() {
      RestResponse response = join(target.getClient().cache(CACHE_NAME).sourceConnected());
      assertEquals(404, response.getStatus());
   }
}
