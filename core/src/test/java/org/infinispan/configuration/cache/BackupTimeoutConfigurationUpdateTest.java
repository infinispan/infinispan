package org.infinispan.configuration.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.commons.api.CacheContainerAdmin.AdminFlag.VOLATILE;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "configuration.cache.BackupTimeoutConfigurationUpdateTest")
public class BackupTimeoutConfigurationUpdateTest extends MultipleCacheManagersTest {
   private static final String CACHE_NAME = "backup-timeouts";
   private static final String TIMEOUT_ATTRIBUTE = "sites.backups.backup.timeout";

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager(TransportFlags.minimalXsiteFlags());
      addClusterEnabledCacheManager(TransportFlags.minimalXsiteFlags());
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.sites().addBackup().site("LON").strategy(BackupConfiguration.BackupStrategy.SYNC).replicationTimeout(5000);
      manager(0).administration().withFlags(VOLATILE).createCache(CACHE_NAME, builder.build());
      waitForClusterToForm(CACHE_NAME);
   }

   public void testRuntimeUpdate() {
      for (String timeout : new String[] {"0", "-1", "0.1ms"}) {
         assertThatThrownBy(() -> manager(0).administration().withFlags(VOLATILE)
               .updateConfigurationAttribute(CACHE_NAME, TIMEOUT_ATTRIBUTE, timeout))
               .isInstanceOf(CacheConfigurationException.class)
               .hasMessageContaining("ISPN000951");
         assertBackupTimeout(5000);
      }

      for (String timeout : new String[] {"15s", "16s"}) {
         assertThatThrownBy(() -> manager(0).administration().withFlags(VOLATILE)
               .updateConfigurationAttribute(CACHE_NAME, TIMEOUT_ATTRIBUTE, timeout))
               .isInstanceOf(CacheConfigurationException.class)
               .hasMessageContaining("ISPN000745");
         assertBackupTimeout(5000);
      }

      for (String timeout : new String[] {"5s", "4s"}) {
         assertThatThrownBy(() -> manager(0).administration().withFlags(VOLATILE)
               .updateConfigurationAttribute(CACHE_NAME, "locking.acquire-timeout", timeout))
               .isInstanceOf(CacheConfigurationException.class)
               .hasMessageContaining("ISPN000745");
         for (int i = 0; i < 2; ++i) {
            assertThat(cache(i, CACHE_NAME).getCacheConfiguration().locking().lockAcquisitionTimeout())
                  .isEqualTo(15000);
         }
      }

      manager(0).administration().withFlags(VOLATILE)
            .updateConfigurationAttribute(CACHE_NAME, TIMEOUT_ATTRIBUTE, "6s");
      assertBackupTimeout(6000);
   }

   private void assertBackupTimeout(long timeout) {
      for (int i = 0; i < 2; ++i) {
         assertThat(cache(i, CACHE_NAME).getCacheConfiguration().sites().allBackups().get(0).replicationTimeout())
               .isEqualTo(timeout);
      }
   }
}
