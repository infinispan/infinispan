package org.infinispan.cli.interpreter;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

@Test(testName = "cli.interpreter.UpgradeTest", groups = "functional")
public class UpgradeTest extends AbstractInfinispanTest {

   private HotRodServer sourceServer;
   private HotRodServer targetServer;
   private EmbeddedCacheManager sourceContainer;
   private Cache<byte[], byte[]> sourceServerCache;
   private EmbeddedCacheManager targetContainer;
   private Cache<byte[], byte[]> targetServerCache;
   private RemoteCacheManager sourceRemoteCacheManager;
   private RemoteCache<String, String> sourceRemoteCache;
   private RemoteCacheManager targetRemoteCacheManager;
   private RemoteCache<String, String> targetRemoteCache;

   @BeforeClass
   public void setup() throws Exception {
      ConfigurationBuilder serverBuilder = hotRodCacheConfiguration(
            TestCacheManagerFactory.getDefaultCacheConfiguration(false));
      sourceContainer = TestCacheManagerFactory.createCacheManager(serverBuilder);
      sourceServerCache = sourceContainer.getCache();
      sourceServer = TestHelper.startHotRodServer(sourceContainer);

      ConfigurationBuilder targetConfigurationBuilder = hotRodCacheConfiguration(
            TestCacheManagerFactory.getDefaultCacheConfiguration(false));
      targetConfigurationBuilder.persistence().addStore(RemoteStoreConfigurationBuilder.class).hotRodWrapping(true).addServer().host("localhost").port(sourceServer.getPort());

      targetContainer = TestCacheManagerFactory.createCacheManager(targetConfigurationBuilder);
      targetServerCache = targetContainer.getCache();
      targetServer = TestHelper.startHotRodServer(targetContainer);

      sourceRemoteCacheManager = new RemoteCacheManager(
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder()
                  .addServers("localhost:" + sourceServer.getPort()).build());
      sourceRemoteCacheManager.start();
      sourceRemoteCache = sourceRemoteCacheManager.getCache();

      targetRemoteCacheManager = new RemoteCacheManager(
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder()
                  .addServers("localhost:" + sourceServer.getPort()).build());
      targetRemoteCacheManager.start();
      targetRemoteCache = targetRemoteCacheManager.getCache();
   }

   public void testSynchronization() throws Exception {
      // Fill the old cluster with data
      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         sourceRemoteCache.put(s, s);
      }
      Interpreter sourceInterpreter = getInterpreter(sourceContainer);

      String sourceSessionId = sourceInterpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      Map<String, String> dumpKeysResult = sourceInterpreter.execute(sourceSessionId, "upgrade --dumpkeys --all;");
      checkNoErrors(dumpKeysResult);

      Interpreter targetInterpreter = getInterpreter(targetContainer);
      String targetSessionId = targetInterpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      Map<String, String> synchronizeResult = targetInterpreter.execute(targetSessionId, "upgrade --synchronize=hotrod --all;");
      checkNoErrors(synchronizeResult);

      Map<String, String> disconnectResult = targetInterpreter.execute(targetSessionId, "upgrade --disconnectsource=hotrod --all;");
      checkNoErrors(disconnectResult);

      assertEquals(sourceServerCache.size() - 1, targetServerCache.size());
   }

   private Interpreter getInterpreter(EmbeddedCacheManager cm) {
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(cm);
      return gcr.getComponent(Interpreter.class);
   }

   private void checkNoErrors(Map<String, String> result) {
      assertFalse(result.get(ResultKeys.ERROR.toString()), result.containsKey(ResultKeys.ERROR.toString()));
   }

   @BeforeMethod
   public void cleanup() {
      sourceServerCache.clear();
      targetServerCache.clear();
   }

   @AfterClass
   public void tearDown() {
      HotRodClientTestingUtil.killRemoteCacheManagers(sourceRemoteCacheManager, targetRemoteCacheManager);
      HotRodClientTestingUtil.killServers(sourceServer, targetServer);
      TestingUtil.killCacheManagers(targetContainer, sourceContainer);
   }

}
