package org.infinispan.configuration.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.commons.api.CacheContainerAdmin.AdminFlag.VOLATILE;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "configuration.cache.RemoteTimeoutConfigurationUpdateTest")
public class RemoteTimeoutConfigurationUpdateTest extends MultipleCacheManagersTest {
   private static final String CACHE_NAME = "timeouts";

   @Override
   protected GlobalConfigurationBuilder defaultGlobalConfigurationBuilder() {
      GlobalConfigurationBuilder builder = super.defaultGlobalConfigurationBuilder();
      builder.transport().distributedSyncTimeout(60000);
      return builder;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager();
      addClusterEnabledCacheManager();
      manager(0).administration().withFlags(VOLATILE)
            .createCache(CACHE_NAME, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false).build());
      waitForClusterToForm(CACHE_NAME);
   }

   public void testRepairRuntimeTimeout() {
      cache(0, CACHE_NAME).getCacheConfiguration().clustering().remoteTimeout(5000);

      manager(0).administration().withFlags(VOLATILE)
            .updateConfigurationAttribute(CACHE_NAME, "clustering.remote-timeout", "30s");
      for (int i = 0; i < 2; ++i) {
         assertThat(cache(i, CACHE_NAME).getCacheConfiguration().clustering().remoteTimeout()).isEqualTo(30000);
         assertThat(TestingUtil.extractComponent(cache(i, CACHE_NAME), RpcManager.class).getSyncRpcOptions().timeout())
               .isEqualTo(30000);
      }
   }

   public void testRuntimeUpdate() {
      manager(0).administration().withFlags(VOLATILE)
            .updateConfigurationAttribute(CACHE_NAME, "clustering.remote-timeout", "30s");
      for (int i = 0; i < 2; ++i) {
         assertThat(cache(i, CACHE_NAME).getCacheConfiguration().clustering().remoteTimeout()).isEqualTo(30000);
         assertThat(TestingUtil.extractComponent(cache(i, CACHE_NAME), RpcManager.class).getSyncRpcOptions().timeout())
               .isEqualTo(30000);
      }

      for (String timeout : new String[] {"0", "-1", "0.1ms", "60s", "61s"}) {
         assertThatThrownBy(() -> manager(0).administration().withFlags(VOLATILE)
               .updateConfigurationAttribute(CACHE_NAME, "clustering.remote-timeout", timeout))
               .isInstanceOf(CacheConfigurationException.class);
         for (int i = 0; i < 2; ++i) {
            assertThat(cache(i, CACHE_NAME).getCacheConfiguration().clustering().remoteTimeout()).isEqualTo(30000);
            assertThat(TestingUtil.extractComponent(cache(i, CACHE_NAME), RpcManager.class).getSyncRpcOptions().timeout())
                  .isEqualTo(30000);
         }
      }
      cache(0, CACHE_NAME).put("key", "value");
      assertThat(cache(1, CACHE_NAME).get("key")).isEqualTo("value");
   }
}
