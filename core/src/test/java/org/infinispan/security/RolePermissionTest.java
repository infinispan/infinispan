package org.infinispan.security;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Set;

import javax.security.auth.Subject;

import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "security.RolePermissionTest")
public class RolePermissionTest extends SingleCacheManagerTest {
   static final Subject ADMIN = TestingUtil.makeSubject("admin");
   static final Subject SUBJECT_A = TestingUtil.makeSubject("A", "role1");
   static final Subject SUBJECT_WITHOUT_PRINCIPAL = TestingUtil.makeSubject();
   AuthorizationManager authzManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable()
            .groupOnlyMapping(false)
            .principalRoleMapper(new IdentityRoleMapper());
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);

      globalRoles
            .role("role1").permission(AuthorizationPermission.EXEC)
            .role("role2").permission(AuthorizationPermission.EXEC)
            .role("role3").permission(AuthorizationPermission.READ, AuthorizationPermission.WRITE)
            .role("role4").permission(AuthorizationPermission.READ, AuthorizationPermission.WRITE)
            .role("role5").permission(AuthorizationPermission.READ, AuthorizationPermission.WRITE)
            .role("admin").permission(AuthorizationPermission.ALL);

      AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();
      authConfig.role("role1").role("role2").role("admin");
      return TestCacheManagerFactory.createCacheManager(global, config);
   }

   @Override
   protected void setup() throws Exception {
      authzManager = Security.doAs(ADMIN, () -> {
         try {
            cacheManager = createCacheManager();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
         if (cache == null) cache = cacheManager.getCache();
         return cache.getAdvancedCache().getAuthorizationManager();
      });
   }

   public void testPermissionAndRole() {
      Security.doAs(SUBJECT_A, () -> {
         authzManager.checkPermission(AuthorizationPermission.EXEC, "role1");
         return null;
      });
   }

   public void testPermissionAndNoRole() {
      Security.doAs(SUBJECT_A, () -> {
         authzManager.checkPermission(AuthorizationPermission.EXEC);
         return null;
      });
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testWrongPermissionAndNoRole() {
      Security.doAs(SUBJECT_A, () -> {
         authzManager.checkPermission(AuthorizationPermission.LISTEN);
         return null;
      });
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testWrongPermissionAndRole() {
      Security.doAs(SUBJECT_A, () -> {
         authzManager.checkPermission(AuthorizationPermission.LISTEN, "role1");
         return null;
      });
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testPermissionAndWrongRole() {
      Security.doAs(SUBJECT_A, () -> {
         authzManager.checkPermission(AuthorizationPermission.EXEC, "role2");
         return null;
      });
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testWrongPermissionAndWrongRole() {
      Security.doAs(SUBJECT_A, () -> {
         authzManager.checkPermission(AuthorizationPermission.LISTEN, "role2");
         return null;
      });
   }

   public void testNoPrincipalInSubject() {
      Security.doAs(SUBJECT_WITHOUT_PRINCIPAL, () -> {
         authzManager.checkPermission(AuthorizationPermission.NONE);
         return null;
      });
   }

   public void testAccessibleCaches() {
      Security.doAs(ADMIN, () -> {
         for (int i = 3; i < 6; i++) {
            ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
            config.security().authorization().enable().role("role" + i).role("admin");
            cacheManager.createCache("cache" + i, config.build());
         }
      });
      Set<String> names = Security.doAs(TestingUtil.makeSubject("Subject34", "role3", "role4"), () -> cacheManager.getAccessibleCacheNames());
      assertEquals(2, names.size());
      assertTrue(names.toString(), names.contains("cache3"));
      assertTrue(names.toString(), names.contains("cache4"));
      names = Security.doAs(TestingUtil.makeSubject("Subject35", "role3", "role5"), () -> cacheManager.getAccessibleCacheNames());
      assertEquals(2, names.size());
      assertTrue(names.toString(), names.contains("cache3"));
      assertTrue(names.toString(), names.contains("cache5"));
      names = Security.doAs(TestingUtil.makeSubject("Subject45", "role4", "role5"), () -> cacheManager.getAccessibleCacheNames());
      assertEquals(2, names.size());
      assertTrue(names.toString(), names.contains("cache4"));
      assertTrue(names.toString(), names.contains("cache5"));
      names = Security.doAs(TestingUtil.makeSubject("Subject0"), () -> cacheManager.getAccessibleCacheNames());
      assertEquals(0, names.size());
   }

   @Override
   protected void teardown() {
      Security.doAs(ADMIN, () -> {
         RolePermissionTest.super.teardown();
         return null;
      });
   }

   @Override
   protected void clearContent() {
      Security.doAs(ADMIN, () -> {
         cacheManager.getCache().clear();
         return null;
      });
   }
}
