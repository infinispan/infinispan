package org.infinispan.persistence.rest.upgrade;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.persistence.rest.configuration.RestStoreConfigurationBuilder;
import org.infinispan.persistence.rest.metadata.MimeMetadataHelper;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.EmbeddedRestServer;
import org.infinispan.rest.RestTestingUtil;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@Test(testName = "persistence.rest.upgrade.RestUpgradeSynchronizerTest", groups = "functional")
public class RestUpgradeSynchronizerTest extends AbstractInfinispanTest {

   private EmbeddedRestServer sourceServer;
   private EmbeddedRestServer targetServer;
   private EmbeddedCacheManager sourceContainer;
   private Cache<byte[], byte[]> sourceServerCache;
   private EmbeddedCacheManager targetContainer;
   private Cache<byte[], byte[]> targetServerCache;
   private HttpClient client;

   @BeforeClass
   public void setup() throws Exception {
      ConfigurationBuilder serverBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      sourceContainer = TestCacheManagerFactory.createCacheManager(serverBuilder);
      sourceServerCache = sourceContainer.getCache();
      sourceServer = RestTestingUtil.startRestServer(sourceContainer);

      ConfigurationBuilder targetConfigurationBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      targetConfigurationBuilder.persistence().addStore(RestStoreConfigurationBuilder.class).host("localhost").port(sourceServer.getPort())
            .path("/rest/" + BasicCacheContainer.DEFAULT_CACHE_NAME).metadataHelper(MimeMetadataHelper.class).rawValues(true).locking().isolationLevel(IsolationLevel.NONE);

      targetContainer = TestCacheManagerFactory.createCacheManager(targetConfigurationBuilder);
      targetServerCache = targetContainer.getCache();
      targetServer = RestTestingUtil.startRestServer(targetContainer, sourceServer.getPort() + 10);

      client = new HttpClient();
   }

   public void testSynchronization() throws Exception {
      // Fill the old cluster with data

      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         PutMethod put = new PutMethod(String.format("http://localhost:%d/rest/%s/%s", sourceServer.getPort(), BasicCacheContainer.DEFAULT_CACHE_NAME, s));
         put.setRequestEntity(new StringRequestEntity(s, "text/plain", "UTF-8"));
         assertEquals(HttpStatus.SC_OK, client.executeMethod(put));
      }
      // Verify access to some of the data from the new cluster
      GetMethod get = new GetMethod(String.format("http://localhost:%d/rest/%s/A", targetServer.getPort(), BasicCacheContainer.DEFAULT_CACHE_NAME));
      assertEquals(HttpStatus.SC_OK, client.executeMethod(get));
      assertEquals("A", get.getResponseBodyAsString());

      RollingUpgradeManager sourceUpgradeManager = sourceServerCache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
      sourceUpgradeManager.recordKnownGlobalKeyset();
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
   public void tearDown() {
      ((SimpleHttpConnectionManager) client.getHttpConnectionManager()).shutdown();
      RestTestingUtil.killServers(sourceServer, targetServer);
      TestingUtil.killCacheManagers(targetContainer, sourceContainer);
   }

}
