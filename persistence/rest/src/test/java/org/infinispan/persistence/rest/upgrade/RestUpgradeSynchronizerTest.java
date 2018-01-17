package org.infinispan.persistence.rest.upgrade;

import static org.testng.AssertJUnit.assertEquals;

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
   private HttpClient client;

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
      targetConfigurationBuilder.persistence().addStore(RestStoreConfigurationBuilder.class).host("localhost").port(sourceServer.getPort())
            .path("/rest/" + BasicCacheContainer.DEFAULT_CACHE_NAME).rawValues(true).locking().isolationLevel(IsolationLevel.NONE);
      targetConfigurationBuilder.encoding().key().mediaType(LEGACY_KEY_ENCODING);

      targetContainer = TestCacheManagerFactory.createServerModeCacheManager(targetConfigurationBuilder);
      targetServerCache = targetContainer.getCache();
      targetServer = new RestServer();
      targetServer.start(restServerConfigurationBuilder.build(), targetContainer);

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

      // Read a newly inserted entry
      GetMethod getInserted = new GetMethod(String.format("http://localhost:%d/rest/%s/A", sourceServer.getPort(), BasicCacheContainer.DEFAULT_CACHE_NAME));
      assertEquals(HttpStatus.SC_OK, client.executeMethod(getInserted));
      assertEquals("A", getInserted.getResponseBodyAsString());

      // Verify access to some of the data from the new cluster
      GetMethod get = new GetMethod(String.format("http://localhost:%d/rest/%s/A", targetServer.getPort(), BasicCacheContainer.DEFAULT_CACHE_NAME));
      assertEquals(HttpStatus.SC_OK, client.executeMethod(get));
      assertEquals("A", get.getResponseBodyAsString());

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
      sourceServer.stop();
      targetServer.stop();
      TestingUtil.killCacheManagers(targetContainer, sourceContainer);
   }

}
