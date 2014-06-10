package org.infinispan.security;

import static org.testng.AssertJUnit.assertEquals;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.impl.ClusterRoleMapper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups="functional", testName="security.ClusterPrincipalMapperTest")
public class ClusterRoleMapperTest extends SingleCacheManagerTest {
   static final Subject ADMIN = TestingUtil.makeSubject("admin");
   static final Subject SUBJECT_A = TestingUtil.makeSubject("A");
   static final Subject SUBJECT_B = TestingUtil.makeSubject("B");
   private ClusterRoleMapper cpm;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable()
            .principalRoleMapper(new ClusterRoleMapper());
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();

      globalRoles
         .role("reader").permission(AuthorizationPermission.ALL_READ)
         .role("writer").permission(AuthorizationPermission.ALL_WRITE)
         .role("admin").permission(AuthorizationPermission.ALL);
      authConfig.role("reader").role("writer").role("admin");
      return TestCacheManagerFactory.createCacheManager(global, config);
   }

   @Override
   protected void setup() throws Exception {
      cpm = Security.doAs(ADMIN, new PrivilegedExceptionAction<ClusterRoleMapper>() {
         @Override
         public ClusterRoleMapper run() throws Exception {
            cacheManager = createCacheManager();
            cpm = (ClusterRoleMapper) cacheManager.getCacheManagerConfiguration().security().authorization().principalRoleMapper();
            cpm.grant("admin", "admin");
            cache = cacheManager.getCache();
            return cpm;
         }
      });
   }

   public void testClusterPrincipalMapper() {
      cpm.grant("writer", "A");
      Security.doAs(SUBJECT_A, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            cacheManager.getCache().put("key", "value");
            return null;
         }

      });
      cpm.grant("reader", "B");
      Security.doAs(SUBJECT_B, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            assertEquals("value", cacheManager.getCache().get("key"));
            return null;
         }

      });

   }

   @Override
   protected void teardown() {
      Security.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            ClusterRoleMapperTest.super.teardown();
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
