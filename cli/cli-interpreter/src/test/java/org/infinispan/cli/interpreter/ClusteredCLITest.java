package org.infinispan.cli.interpreter;

import static org.infinispan.test.fwk.TestCacheManagerFactory.configureGlobalJmx;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups="functional", testName = "cli.interpreter.ClusteredCLITest")
public class ClusteredCLITest extends MultipleCacheManagersTest {

   public static String CACHE_NAME = "distCache";

   MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   public ClusteredCLITest() {
      cleanup = CleanupPhase.AFTER_TEST;
   }

   @Override
   protected void createCacheManagers() {
      for (int i = 0; i < 2; i++) {
         GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
         global.cacheContainer().defaultCache(CACHE_NAME);
         configureGlobalJmx(global, getClass().getSimpleName() + i, mBeanServerLookup);
         ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
         addClusterEnabledCacheManager(global, builder);
      }
      waitForClusterToForm(cacheName());
   }

   protected String cacheName() {
      return CACHE_NAME;
   }

   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }

   public void testCreateCluster() throws Exception {
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(manager(0));
      Interpreter interpreter = gcr.getComponent(Interpreter.class);
      String sessionId = interpreter.createSessionId(null);
      interpreter.execute(sessionId, String.format("create anothercache like %s;", cacheName()));
      assert manager(0).cacheExists("anothercache");
      assert manager(1).cacheExists("anothercache");
   }
}
