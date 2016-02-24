package org.infinispan.security;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups="functional", testName="security.RolePermissionTest")
public class RolePermissionTest extends SingleCacheManagerTest {
   static final Subject ADMIN = TestingUtil.makeSubject("admin");
   static final Subject SUBJECT_A = TestingUtil.makeSubject("A", "role1");
   static final Subject SUBJECT_WITHOUT_PRINCIPAL = TestingUtil.makeSubject();
   AuthorizationManager authzManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable()
            .principalRoleMapper(new IdentityRoleMapper());
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();

      globalRoles
         .role("role1").permission(AuthorizationPermission.EXEC)
         .role("role2").permission(AuthorizationPermission.EXEC)
         .role("admin").permission(AuthorizationPermission.ALL);
      authConfig.role("role1").role("role2").role("admin");
      return TestCacheManagerFactory.createCacheManager(global, config);
   }

   @Override
   protected void setup() throws Exception {
      authzManager = Security.doAs(ADMIN, new PrivilegedExceptionAction<AuthorizationManager>() {

         @Override
         public AuthorizationManager run() throws Exception {
            cacheManager = createCacheManager();
            if (cache == null) cache = cacheManager.getCache();
            return cache.getAdvancedCache().getAuthorizationManager();
         }
      });
   }

   public void testPermissionAndRole() {
      Security.doAs(SUBJECT_A, new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            authzManager.checkPermission(AuthorizationPermission.EXEC, "role1");
            return null;
         }
      });
   }

   public void testPermissionAndNoRole() {
      Security.doAs(SUBJECT_A, new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            authzManager.checkPermission(AuthorizationPermission.EXEC);
            return null;
         }
      });
   }

   @Test(expectedExceptions=SecurityException.class)
   public void testWrongPermissionAndNoRole() {
      Security.doAs(SUBJECT_A, new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            authzManager.checkPermission(AuthorizationPermission.LISTEN);
            return null;
         }
      });
   }

   @Test(expectedExceptions=SecurityException.class)
   public void testWrongPermissionAndRole() {
      Security.doAs(SUBJECT_A, new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            authzManager.checkPermission(AuthorizationPermission.LISTEN, "role1");
            return null;
         }
      });
   }

   @Test(expectedExceptions=SecurityException.class)
   public void testPermissionAndWrongRole() {
      Security.doAs(SUBJECT_A, new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            authzManager.checkPermission(AuthorizationPermission.EXEC, "role2");
            return null;
         }
      });
   }

   @Test(expectedExceptions=SecurityException.class)
   public void testWrongPermissionAndWrongRole() {
      Security.doAs(SUBJECT_A, new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            authzManager.checkPermission(AuthorizationPermission.LISTEN, "role2");
            return null;
         }
      });
   }

   @Test(expectedExceptions=SecurityException.class)
   public void testNoPrincipalInSubject() {
      Security.doAs(SUBJECT_WITHOUT_PRINCIPAL, new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            authzManager.checkPermission(AuthorizationPermission.NONE);
            return null;
         }
      });
   }

   @Override
   protected void teardown() {
      Security.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            RolePermissionTest.super.teardown();
            return null;
         }
      });
   }

   @Override
   protected void clearContent() {
      Security.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            cacheManager.getCache().clear();
            return null;
         }
      });
   }
}
