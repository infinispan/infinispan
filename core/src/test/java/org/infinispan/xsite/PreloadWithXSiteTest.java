package org.infinispan.xsite;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;

import org.infinispan.Cache;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Tests if preload happens successfully when xsite is configured.
 * <p>
 * JIRA: ISPN-7265
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "xsite", testName = "xsite.PreloadWithXSiteTest")
public class PreloadWithXSiteTest extends AbstractTwoSitesTest {

   private static final String NYC_CACHE_STORE_NAME = "nyc-dummy-cache-store";
   private static final String LON_CACHE_STORE_NAME = "lon-dummy-cache-store";
   private static final int NR_KEYS = 5;

   public PreloadWithXSiteTest() {
      implicitBackupCache = true;
      initialClusterSize = 2;
   }

   public void testPreload(Method method) {
      for (int i = 0; i < NR_KEYS; ++i) {
         cache(NYC, 0).put(TestingUtil.k(method, i), TestingUtil.v(method, i));
      }
      assertData(method);
      stopNYC();
      reCreateNYC();
      assertData(method);
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true)
            .storeName(NYC_CACHE_STORE_NAME);
      return builder;
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true)
            .storeName(LON_CACHE_STORE_NAME);
      return builder;
   }

   private void stopNYC() {
      site(NYC).cacheManagers.forEach(Lifecycle::stop);
   }

   private void reCreateNYC() {
      ConfigurationBuilder nyc = getNycActiveConfig();
      nyc.sites().addBackup()
            .site(LON)
            .strategy(BackupConfiguration.BackupStrategy.SYNC)
            .sites().addInUseBackupSite(LON);
      createSite(NYC, initialClusterSize, globalConfigurationBuilderForSite(NYC), nyc);
   }

   private void assertData(Method method) {
      assertDataForSite(method, NYC);
      assertDataForSite(method, LON);
   }

   private void assertDataForSite(Method method, String site) {
      for (Cache<String, String> cache : this.<String, String>caches(site)) {
         for (int i = 0; i < NR_KEYS; ++i) {
            assertEquals("Cache=" + addressOf(cache), TestingUtil.v(method, i), cache.get(TestingUtil.k(method, i)));
         }
      }
   }
}
