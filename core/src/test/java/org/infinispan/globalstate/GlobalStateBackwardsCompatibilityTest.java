package org.infinispan.globalstate;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * ISPN-12667 A test to ensure that global state properties from previous Infinispan majors are compatible.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
@Test(groups = "functional", testName = "globalstate.GlobalStateBackwardsCompatibilityTest")
public class GlobalStateBackwardsCompatibilityTest extends MultipleCacheManagersTest {
   private static final String CACHE_NAME = "testCache";
   private static final String MEMBER_0 = UUID.randomUUID().toString();
   private static final String MEMBER_1 = UUID.randomUUID().toString();

   @Override
   protected void createCacheManagers() throws Throwable {
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      super.destroy();
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
   }

   public void testCreateClusterWithGlobalState11() throws Exception {
      createCacheManagerWithGlobalState(MEMBER_0, tmpDirectory(this.getClass().getSimpleName(), "0"));
      createCacheManagerWithGlobalState(MEMBER_1, tmpDirectory(this.getClass().getSimpleName(), "1"));
      waitForClusterToForm(CACHE_NAME);
   }

   private void createCacheManagerWithGlobalState(String uuid, String stateDirectory) throws Exception {
      new File(stateDirectory).mkdirs();
      createCacheState(stateDirectory);

      Properties globalState = new Properties();
      globalState.put("@version", "11.0.9.Final");
      globalState.put("version-major", "11");
      globalState.put("@timestamp", "2021-01-28T10\\:53\\:56.289272Z");
      globalState.put("uuid", uuid);
      globalState.store(new FileOutputStream(new File(stateDirectory, "___global.state")), null);

      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);

      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.REPL_SYNC).stateTransfer().timeout(30, TimeUnit.SECONDS)
            .persistence().addSoftIndexFileStore();
      EmbeddedCacheManager manager = addClusterEnabledCacheManager(global, null);
      manager.defineConfiguration(CACHE_NAME, config.build());
   }

   private void createCacheState(String stateDirectory) throws Exception {
      Properties cacheState = new Properties();
      cacheState.put("@version", "11.0.9.Final");
      cacheState.put("@timestamp", "2021-01-28T10\\:53\\:56.289272Z");
      cacheState.put("version-major", "11");
      cacheState.put("consistentHash", "org.infinispan.distribution.ch.impl.ReplicatedConsistentHash");
      cacheState.put("members", "2");
      cacheState.put("member.0", MEMBER_0);
      cacheState.put("member.1", MEMBER_1);
      cacheState.put("primaryOwners", "256");
      IntStream.range(0, 256).forEach(i -> cacheState.put("primaryOwners." + i, Integer.toString(i % 2)));
      cacheState.store(new FileOutputStream(new File(stateDirectory, CACHE_NAME + ".state")), null);
   }
}
