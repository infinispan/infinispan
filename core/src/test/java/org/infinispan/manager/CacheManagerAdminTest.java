package org.infinispan.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.testing.Exceptions;
import org.testng.annotations.Test;

@Test(testName = "manager.CacheManagerAdminTest", groups = "functional")
@CleanupAfterMethod
public class CacheManagerAdminTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().configurationStorage(ConfigurationStorage.VOLATILE);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);
   }

   public void testClusterCacheTest() {
      waitForClusterToForm();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      Configuration configuration = builder.build();
      Cache<Object, Object> cache = manager(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache("a", configuration);

      waitForClusterToForm("a");
      assertEquals(cacheManagers.size(), cache.getAdvancedCache().getRpcManager().getMembers().size());
      checkConsistencyAcrossCluster("a", configuration);

      addClusterEnabledCacheManager();
      checkConsistencyAcrossCluster("a", configuration);

      Exceptions.expectException(CacheConfigurationException.class,
            "ISPN000374: No such template 'nonExistingTemplate' when declaring 'b'",
            () -> manager(0).administration().createCache("b", "nonExistingTemplate"));

      // attempt to create an existing cache
      Exceptions.expectException(CacheConfigurationException.class,
            "ISPN000507: Cache a already exists",
            () -> manager(0).administration().createCache("a", configuration));

      // getOrCreate should work
      manager(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).getOrCreateCache("a", configuration);

      manager(1).administration().removeCache("a");
      checkCacheExistenceAcrossCluster("a", false);

      addClusterEnabledCacheManager();
      checkCacheExistenceAcrossCluster("a", false);
   }

   public void testGetCacheConfiguration(Method m) {
      EmbeddedCacheManager ecm = manager(0);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);

      String alias = "alias-" + this.hashCode();
      builder.aliases(alias);
      ecm.defineConfiguration(m.getName(), builder.build());

      assertThat(ecm.getCacheConfiguration(m.getName())).isNotNull();
      assertThat(ecm.getCacheConfiguration(alias)).isNotNull();
      assertThat(ecm.getCacheConfiguration("something-that-doesnt-exist")).isNull();
   }

   protected void checkCacheExistenceAcrossCluster(String cacheName, boolean exists) {
      for (EmbeddedCacheManager m : cacheManagers) {
         if (exists) {
            assertTrue("Cache '" + cacheName + "' should be present on " + m, m.cacheExists(cacheName));
         } else {
            assertFalse("Cache '" + cacheName + "' should NOT be present on " + m, m.cacheExists(cacheName));
         }
      }
   }

   protected void checkConsistencyAcrossCluster(String cacheName, Configuration configuration) {
      for (EmbeddedCacheManager m : cacheManagers) {
         Configuration actualConfiguration = m.getCacheConfiguration(cacheName);
         assertNotNull("Cache " + cacheName + " missing from " + m, actualConfiguration);
         assertEquals(configuration, actualConfiguration);
         Cache<Object, Object> cache = m.getCache(cacheName);
         assertEquals(cacheManagers.size(), cache.getAdvancedCache().getRpcManager().getMembers().size());
      }
   }

}
