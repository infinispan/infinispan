package org.infinispan.security;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.security.mappers.ClusterPermissionMapper;
import org.infinispan.security.mappers.ClusterRoleMapper;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups="functional", testName="security.DynamicRBACTest")
public class DynamicRBACTest extends MultipleCacheManagersTest {
   static final Subject ADMIN = TestingUtil.makeSubject("admin");
   static final Subject SUBJECT_A = TestingUtil.makeSubject("A");
   static final Subject SUBJECT_B = TestingUtil.makeSubject("B");
   private ClusterRoleMapper crm;
   private ClusterPermissionMapper cpm;


   @Override
   protected void createCacheManagers() throws Throwable {
      Security.doAs(ADMIN, (PrivilegedExceptionAction<Void>) () -> {
         addClusterEnabledCacheManager(getGlobalConfigurationBuilder(), getConfigurationBuilder());
         addClusterEnabledCacheManager(getGlobalConfigurationBuilder(), getConfigurationBuilder());
         waitForClusterToForm();
         crm = (ClusterRoleMapper) cacheManagers.get(0).getCacheManagerConfiguration().security().authorization().principalRoleMapper();
         crm.grant("admin", "admin");
         cpm = (ClusterPermissionMapper) cacheManagers.get(0).getCacheManagerConfiguration().security().authorization().rolePermissionMapper();
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

   private GlobalConfigurationBuilder getGlobalConfigurationBuilder() {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable()
            .principalRoleMapper(new ClusterRoleMapper())
            .rolePermissionMapper(new ClusterPermissionMapper());

      globalRoles
            .role("reader").permission(AuthorizationPermission.ALL_READ)
            .role("writer").permission(AuthorizationPermission.ALL_WRITE)
            .role("admin").permission(AuthorizationPermission.ALL);
      return global;
   }

   public void testClusterPrincipalMapper() {
      crm.grant("writer", "A");
      Security.doAs(SUBJECT_A, (PrivilegedAction<Void>) () -> {
         cacheManagers.get(0).getCache().put("key", "value");
         return null;
      });
      crm.grant("reader", "B");
      Security.doAs(SUBJECT_B, (PrivilegedAction<Void>) () -> {
         assertEquals("value", cacheManagers.get(0).getCache().get("key"));
         return null;
      });
   }

   public void testClusterPermissionMapper() {
      Map<String, Role> roles = cpm.getAllRoles();
      assertEquals(0, roles.size());
      await(cpm.addRole(Role.newRole("wizard", true, AuthorizationPermission.ALL_WRITE)));
      await(cpm.addRole(Role.newRole("cleric", true, AuthorizationPermission.ALL_READ)));
      roles = cpm.getAllRoles();
      assertEquals(2, roles.size());
      assertTrue(roles.containsKey("wizard"));
      assertTrue(roles.containsKey("cleric"));
      Cache<String, String> cpmCache = Security.doAs(ADMIN, (PrivilegedAction<Cache<String, String>>) () -> {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.security().authorization().enable().roles("admin", "wizard", "cleric");
         return cacheManagers.get(0).createCache("cpm", builder.build(cacheManagers.get(0).getCacheManagerConfiguration()));
      });
      Security.doAs(TestingUtil.makeSubject("wizard"), (PrivilegedAction<Void>) () -> {
         cpmCache.put("key", "value");
         return null;
      });
      Security.doAs(TestingUtil.makeSubject("cleric"), (PrivilegedAction<Void>) () -> {
         assertEquals("value", cpmCache.get("key"));
         return null;
      });
      await(cpm.removeRole("cleric"));
      roles = cpm.getAllRoles();
      assertEquals(1, roles.size());
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      Security.doAs(ADMIN, (PrivilegedAction<Void>) () -> {
         DynamicRBACTest.super.destroy();
         return null;
      });
   }

   @Override
   protected void clearContent() {
      Security.doAs(ADMIN, (PrivilegedAction<Void>) () -> {
         cacheManagers.forEach(cm -> cm.getCache().clear());
         return null;
      });
   }
}
