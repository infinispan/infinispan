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
import org.junit.Assert;
import org.testng.annotations.Test;

@Test(groups="functional", testName="security.SimpleRolePermissionTest")
public class SimpleRolePermissionTest extends SingleCacheManagerTest {

   private static final Subject ADMIN = TestingUtil.makeSubject("diego");

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
      Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {

         @Override
         public Void run() throws Exception {
            cacheManager = createCacheManager();
            if (cache == null) {
               //cache = cacheManager.getCache();
               cache = cacheManager.getCache("fooCache", true);
            }
            return null;
         }
      });
   }

   @Override
   protected void clearCacheManager() throws Exception {
      Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {

         @Override
         public Void run() throws Exception {
            SimpleRolePermissionTest.super.clearCacheManager();
            return null;
         }
      });
   }

   public void testPutData() {
      Security.doAs(ADMIN, new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            cache.put("foo", "bar");
            Assert.assertEquals("bar", cache.get("foo"));
            return null;
         }
      });
   }
}
