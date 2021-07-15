package org.infinispan.server.functional;

import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.junit.Assert.assertEquals;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.upgrade.SerializationUtils;
import org.junit.Test;

/**
 * @since 13.0
 */
public class RollingUpgradeDynamicStoreIT extends RollingUpgradeIT {

   static final ParserRegistry parserRegistry = new ParserRegistry();

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
      connectTargetCluster();

      // Make sure data is accessible from the target cluster
      assertEquals("name-13", getPersonName("13", restClientTarget));

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

   private void createTargetClusterWithoutStore() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      createCache(CACHE_NAME, builder, target.getClient());
   }

   protected void connectTargetCluster() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      addRemoteStore(builder);

      RemoteStoreConfiguration remoteStore = (RemoteStoreConfiguration) builder.build().persistence().stores().iterator().next();

      RestEntity restEntity = RestEntity.create(MediaType.APPLICATION_JSON, SerializationUtils.toJson(remoteStore));
      RestResponse response = join(target.getClient().cache(CACHE_NAME).connectSource(restEntity));
      assertEquals(response.getBody(), 204, response.getStatus());
   }
}
