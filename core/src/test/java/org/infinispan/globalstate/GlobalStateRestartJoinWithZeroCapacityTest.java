package org.infinispan.globalstate;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.topology.PersistentUUID;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * ISPN-13729 A test to ensure that zero-capacity REPL_SYNC caches can join a cluster which has restarted from persistent state.
 *
 * @author Ryan Emerson
 * @since 14.0
 */
@Test(groups = "functional", testName = "globalstate.GlobalStateRestartJoinWithZeroCapacityTest")
public class GlobalStateRestartJoinWithZeroCapacityTest extends MultipleCacheManagersTest {
   private static final String CACHE_NAME = "testCache";
   private static final String MEMBER_0 = PersistentUUID.randomUUID().toString();
   private static final String MEMBER_1 = PersistentUUID.randomUUID().toString();

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
      createZeroCapacityManager(tmpDirectory(this.getClass().getSimpleName(), "2"));
      waitForClusterToForm(CACHE_NAME);
   }

   private void createCacheManagerWithGlobalState(String uuid, String stateDirectory) throws Exception {
      new File(stateDirectory).mkdirs();
      createCacheState(stateDirectory);

      Properties globalState = new Properties();
      globalState.put("@version", "13.0.5.Final");
      globalState.put("version-major", "13");
      globalState.put("@timestamp", "2022-02-24T11\\:57\\:30.530659Z");
      globalState.put("uuid", uuid);
      globalState.store(new FileOutputStream(new File(stateDirectory, "___global.state")), null);

      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);

      EmbeddedCacheManager manager = addClusterEnabledCacheManager(global, null);
      manager.defineConfiguration(CACHE_NAME, cacheConfig());
   }

   private void createZeroCapacityManager(String stateDirectory) {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.zeroCapacityNode(true)
            .globalState().enable().persistentLocation(stateDirectory);

      EmbeddedCacheManager manager = addClusterEnabledCacheManager(global, null);
      manager.defineConfiguration(CACHE_NAME, cacheConfig());
   }

   private Configuration cacheConfig() {
      return new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.REPL_SYNC).stateTransfer().timeout(30, TimeUnit.SECONDS)
            .build();
   }

   private void createCacheState(String stateDirectory) throws Exception {
      Properties cacheState = new Properties();
      cacheState.put("@version", "13.0.5.Final");
      cacheState.put("@timestamp", "2022-02-24T11\\:57\\:29.977881Z");
      cacheState.put("version-major", "13");
      cacheState.put("consistentHash", "org.infinispan.distribution.ch.impl.ReplicatedConsistentHash");
      cacheState.put("members", "1");
      cacheState.put("member.0", MEMBER_0);
      cacheState.put("primaryOwners", "256");
      IntStream.range(0, 256).forEach(i -> cacheState.put("primaryOwners." + i, "0"));
      cacheState.store(new FileOutputStream(new File(stateDirectory, CACHE_NAME + ".state")), null);
   }
}
