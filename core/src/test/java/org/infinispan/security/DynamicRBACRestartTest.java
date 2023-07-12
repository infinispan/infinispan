package org.infinispan.security;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.security.mappers.ClusterPermissionMapper;
import org.infinispan.security.mappers.ClusterRoleMapper;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "security.DynamicRBACRestartTest")
public class DynamicRBACRestartTest extends MultipleCacheManagersTest {
   static final Subject ADMIN = TestingUtil.makeSubject("admin");
   private ClusterRoleMapper crm;
   private ClusterPermissionMapper cpm;


   @Override
   protected void createCacheManagers() throws Throwable {
      Security.doAs(ADMIN, () -> {
         addClusterEnabledCacheManager(getGlobalConfigurationBuilder("A", true), getConfigurationBuilder());
         addClusterEnabledCacheManager(getGlobalConfigurationBuilder("B", true), getConfigurationBuilder());
         waitForClusterToForm();
         crm = (ClusterRoleMapper) cacheManagers.get(0).getCacheManagerConfiguration().security().authorization().principalRoleMapper();
         crm.grant("admin", "admin");
         cpm = (ClusterPermissionMapper) cacheManagers.get(0).getCacheManagerConfiguration().security().authorization().rolePermissionMapper();
         await(cpm.addRole(Role.newRole("wizard", true, AuthorizationPermission.ALL_WRITE)));
         await(cpm.addRole(Role.newRole("cleric", true, AuthorizationPermission.ALL_READ)));
         return null;
      });
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.DIST_SYNC);
      AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();
      authConfig.role("reader").role("writer").role("admin");
      return config;
   }

   private GlobalConfigurationBuilder getGlobalConfigurationBuilder(String id, boolean clear) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), id);
      if (clear)
         Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory).
            configurationStorage(ConfigurationStorage.OVERLAY);
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable()
            .groupOnlyMapping(false)
            .principalRoleMapper(new ClusterRoleMapper())
            .rolePermissionMapper(new ClusterPermissionMapper());

      globalRoles
            .role("reader").permission(AuthorizationPermission.ALL_READ)
            .role("writer").permission(AuthorizationPermission.ALL_WRITE)
            .role("admin").permission(AuthorizationPermission.ALL);
      return global;
   }

   public void testPermissionsRestart() {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.DIST_SYNC);
      AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();
      authConfig.role("admin").role("wizard").role("cleric");
      Security.doAs(ADMIN, () ->
            cacheManagers.get(0).administration().createCache("minastirith", config.build())
      );
      Security.doAs(ADMIN, () -> TestingUtil.killCacheManagers(cacheManagers));
      cacheManagers.clear();
      Security.doAs(ADMIN, () -> {
         addClusterEnabledCacheManager(getGlobalConfigurationBuilder("A", false), getConfigurationBuilder());
         addClusterEnabledCacheManager(getGlobalConfigurationBuilder("B", false), getConfigurationBuilder());
         waitForClusterToForm();
      });
      AdvancedCache<Object, Object> cache = manager(0).getCache("minastirith").getAdvancedCache();
      cache.withSubject(TestingUtil.makeSubject("wizard")).put("k1", "v1");
      assertEquals("v1", cache.withSubject(TestingUtil.makeSubject("cleric")).get("k1"));
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      Security.doAs(ADMIN, () -> {
         DynamicRBACRestartTest.super.destroy();
         return null;
      });
   }

   @Override
   protected void clearContent() {
      Security.doAs(ADMIN, () -> {
         cacheManagers.forEach(cm -> cm.getCache().clear());
         return null;
      });
   }
}
