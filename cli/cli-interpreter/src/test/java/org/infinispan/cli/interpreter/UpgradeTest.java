package org.infinispan.cli.interpreter;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "cli.interpreter.UpgradeTest", groups = "functional")
public class UpgradeTest extends AbstractInfinispanTest {

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();
   private HotRodServer sourceServer;
   private HotRodServer targetServer;
   private EmbeddedCacheManager sourceContainer;
   private Cache<byte[], byte[]> sourceServerCache;
   private EmbeddedCacheManager targetContainer;
   private Cache<byte[], byte[]> targetServerCache;
   private RemoteCacheManager sourceRemoteCacheManager;
   private RemoteCache<String, String> sourceRemoteCache;
   private RemoteCacheManager targetRemoteCacheManager;

   @BeforeClass
   public void setup() {
      ConfigurationBuilder serverBuilder = hotRodCacheConfiguration(
            TestCacheManagerFactory.getDefaultCacheConfiguration(false));
      GlobalConfigurationBuilder sourceGlobal = new GlobalConfigurationBuilder().nonClusteredDefault();
      configureJmx(sourceGlobal, getClass().getSimpleName() + "-source", mBeanServerLookup);
      sourceContainer = TestCacheManagerFactory.createCacheManager(sourceGlobal, serverBuilder);
      sourceServerCache = sourceContainer.getCache();
      sourceServer = HotRodClientTestingUtil.startHotRodServer(sourceContainer);

      ConfigurationBuilder targetConfigurationBuilder = hotRodCacheConfiguration(
            TestCacheManagerFactory.getDefaultCacheConfiguration(false));
      targetConfigurationBuilder.persistence().addStore(RemoteStoreConfigurationBuilder.class).hotRodWrapping(true).addServer().host("localhost").port(sourceServer.getPort());

      GlobalConfigurationBuilder targetGlobal = new GlobalConfigurationBuilder().nonClusteredDefault();
      configureJmx(targetGlobal, getClass().getSimpleName() + "-target", mBeanServerLookup);
      targetContainer = TestCacheManagerFactory.createCacheManager(targetGlobal, targetConfigurationBuilder);
      targetServerCache = targetContainer.getCache();
      targetServer = HotRodClientTestingUtil.startHotRodServer(targetContainer);

      sourceRemoteCacheManager = new RemoteCacheManager(
         HotRodClientTestingUtil.newRemoteConfigurationBuilder(sourceServer).build());
      sourceRemoteCacheManager.start();
      sourceRemoteCache = sourceRemoteCacheManager.getCache();

      targetRemoteCacheManager = new RemoteCacheManager(
         HotRodClientTestingUtil.newRemoteConfigurationBuilder(sourceServer).build());
      targetRemoteCacheManager.start();
   }

   public void testSynchronization() throws Exception {
      // Fill the old cluster with data
      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         sourceRemoteCache.put(s, s);
      }

      Interpreter targetInterpreter = getInterpreter(targetContainer);
      String targetSessionId = targetInterpreter.createSessionId(targetServer.getCacheManager().getCacheManagerConfiguration().defaultCacheName().get());
      Map<String, String> synchronizeResult = targetInterpreter.execute(targetSessionId, "upgrade --synchronize=hotrod;");
      checkNoErrors(synchronizeResult);

      Map<String, String> disconnectResult = targetInterpreter.execute(targetSessionId, "upgrade --disconnectsource=hotrod;");
      checkNoErrors(disconnectResult);

      assertEquals(sourceServerCache.size(), targetServerCache.size());
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
