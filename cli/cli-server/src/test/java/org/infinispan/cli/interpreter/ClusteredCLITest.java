package org.infinispan.cli.interpreter;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups="functional", testName = "cli-server.ClusteredCLITest")
public class ClusteredCLITest extends MultipleCacheManagersTest {

   public static String CACHE_NAME = "distCache";

   public ClusteredCLITest() {
      cleanup = CleanupPhase.AFTER_TEST;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      createClusteredCaches(2, cacheName(), builder);
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
