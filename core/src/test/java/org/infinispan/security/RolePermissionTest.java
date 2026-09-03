package org.infinispan.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;

import javax.security.auth.Subject;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeListener;
import org.infinispan.configuration.cache.AuthorizationConfiguration;
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
      assertTrue(names.contains("cache3"), names.toString());
      assertTrue(names.contains("cache4"), names.toString());
      names = Security.doAs(TestingUtil.makeSubject("Subject35", "role3", "role5"), () -> cacheManager.getAccessibleCacheNames());
      assertEquals(2, names.size());
      assertTrue(names.contains("cache3"), names.toString());
      assertTrue(names.contains("cache5"), names.toString());
      names = Security.doAs(TestingUtil.makeSubject("Subject45", "role4", "role5"), () -> cacheManager.getAccessibleCacheNames());
      assertEquals(2, names.size());
      assertTrue(names.contains("cache4"), names.toString());
      assertTrue(names.contains("cache5"), names.toString());
      names = Security.doAs(TestingUtil.makeSubject("Subject0"), () -> cacheManager.getAccessibleCacheNames());
      assertEquals(0, names.size());
   }

   /**
    * Reproducer for https://github.com/infinispan/infinispan/issues/18039
    * <p>
    * Each call to {@code getAccessibleCacheNames()} creates a new {@code AuthorizationManagerImpl} and invokes
    * {@code init()}, which registers a listener on the cache's {@code AuthorizationConfiguration.ROLES} attribute.
    * These listeners are never removed, causing an unbounded memory leak.
    */
   public void testGetAccessibleCacheNamesDoesNotLeakListeners() throws Exception {
      Security.doAs(ADMIN, () -> {
         ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
         config.security().authorization().enable().role("role1").role("admin");
         cacheManager.createCache("leakTestCache", config.build());
      });

      // Retrieve the ROLES attribute directly from the cache configuration (requires ADMIN)
      AuthorizationConfiguration authCfg = Security.doAs(ADMIN,
            () -> cacheManager.getCacheConfiguration("leakTestCache").security().authorization());
      Attribute<Set> rolesAttribute = authCfg.attributes().attribute(AuthorizationConfiguration.ROLES);

      // Access the private listeners field via reflection
      Field listenersField = Attribute.class.getDeclaredField("listeners");
      listenersField.setAccessible(true);

      @SuppressWarnings("unchecked")
      List<AttributeListener<Set>> listeners = (List<AttributeListener<Set>>) listenersField.get(rolesAttribute);
      int listenerCountBefore = listeners == null ? 0 : listeners.size();

      // Call getAccessibleCacheNames() multiple times — each call should NOT add a new listener
      int invocations = 5;
      Subject subject = TestingUtil.makeSubject("leakTestUser", "role1");
      for (int i = 0; i < invocations; i++) {
         Security.doAs(subject, () -> cacheManager.getAccessibleCacheNames());
      }

      int listenerCountAfter = listeners == null ? 0 : listeners.size();

      // If the bug is present, listenerCountAfter will exceed listenerCountBefore by invocations (5).
      // The expected correct behaviour is that no new listeners are added.
      assertEquals(listenerCountBefore, listenerCountAfter,
            "getAccessibleCacheNames() must not register new listeners on the ROLES attribute (issue #18039). " +
                  "Count before: " + listenerCountBefore + ", count after " + invocations + " calls: " + listenerCountAfter);
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
