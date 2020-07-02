package org.infinispan.persistence.rest.upgrade;

import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.rest.configuration.RestStoreConfigurationBuilder;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "persistence.rest.upgrade.RestUpgradeSynchronizerTest", groups = "functional")
public class RestUpgradeSynchronizerTest extends AbstractInfinispanTest {

   private RestServer sourceServer;
   private RestServer targetServer;
   private EmbeddedCacheManager sourceContainer;
   private Cache<byte[], byte[]> sourceServerCache;
   private EmbeddedCacheManager targetContainer;
   private Cache<byte[], byte[]> targetServerCache;
   private RestClient sourceClient;
   private RestClient targetClient;

   protected static final String LEGACY_KEY_ENCODING = "application/x-java-object;type=java.lang.String";

   protected ConfigurationBuilder getSourceServerBuilder() {
      ConfigurationBuilder serverBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      serverBuilder.encoding().key().mediaType(LEGACY_KEY_ENCODING);
      return serverBuilder;
   }

   @BeforeClass
   public void setup() {
      RestServerConfigurationBuilder restServerConfigurationBuilder = new RestServerConfigurationBuilder();
      restServerConfigurationBuilder.port(0);

      ConfigurationBuilder serverBuilder = getSourceServerBuilder();
      sourceContainer = TestCacheManagerFactory.createServerModeCacheManager(serverBuilder);
      sourceServerCache = sourceContainer.getCache();
      sourceServer = new RestServer();
      sourceServer.start(restServerConfigurationBuilder.build(), sourceContainer);


      ConfigurationBuilder targetConfigurationBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      targetConfigurationBuilder.persistence()
            .addStore(RestStoreConfigurationBuilder.class)
            .host("localhost")
            .port(sourceServer.getPort())
            .cacheName(TestingUtil.getDefaultCacheName(sourceContainer))
            .rawValues(true)
            .segmented(false)
            .locking().isolationLevel(IsolationLevel.NONE);
      targetConfigurationBuilder.encoding().key().mediaType(LEGACY_KEY_ENCODING);

      targetContainer = TestCacheManagerFactory.createServerModeCacheManager(targetConfigurationBuilder);
      targetServerCache = targetContainer.getCache();
      targetServer = new RestServer();
      targetServer.start(restServerConfigurationBuilder.build(), targetContainer);

      RestClientConfigurationBuilder builderSource = new RestClientConfigurationBuilder();
      builderSource.addServer().host(sourceServer.getHost()).port(sourceServer.getPort());
      sourceClient = RestClient.forConfiguration(builderSource.build());

      RestClientConfigurationBuilder builderTarget = new RestClientConfigurationBuilder();
      builderTarget.addServer().host(targetServer.getHost()).port(targetServer.getPort());
      targetClient = RestClient.forConfiguration(builderTarget.build());
   }

   public void testSynchronization() throws Exception {
      // Fill the old cluster with data
      RestCacheClient sourceCacheClient = sourceClient.cache(TestingUtil.getDefaultCacheName(sourceContainer));
      RestCacheClient targetCacheClient = targetClient.cache(TestingUtil.getDefaultCacheName(targetContainer));

      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         CompletionStage<RestResponse> response = sourceCacheClient.put(s, RestEntity.create(TEXT_PLAIN, s));
         assertEquals(204, join(response).getStatus());
      }

      // Read a newly inserted entry
      RestResponse response = join(sourceCacheClient.get("A"));
      assertEquals(200, response.getStatus());
      assertEquals("A", response.getBody());

      // Verify access to some of the data from the new cluster
      response = join(targetCacheClient.get("A"));
      assertEquals(200, response.getStatus());
      assertEquals("A", response.getBody());

      RollingUpgradeManager targetUpgradeManager = targetServerCache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
      targetUpgradeManager.synchronizeData("rest");
      assertEquals(sourceServerCache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).size(), targetServerCache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).size());

      targetUpgradeManager.disconnectSource("rest");
   }

   @BeforeMethod
   public void cleanup() {
      sourceServerCache.clear();
      targetServerCache.clear();
   }

   @AfterClass
   public void tearDown() throws IOException {
      sourceClient.close();
      targetClient.close();
      sourceServer.stop();
      targetServer.stop();
      TestingUtil.killCacheManagers(targetContainer, sourceContainer);
   }

}
