package org.infinispan.security;

import static org.testng.AssertJUnit.assertEquals;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "security.ClusteredSecureCacheTest")
public class ClusteredSecureCacheTest extends MultipleCacheManagersTest {
   final static Subject ADMIN = TestingUtil.makeSubject("admin");

   @Override
   protected void createCacheManagers() throws Throwable {
      final GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      final ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC);
      global.security().authorization().enable()
            .principalRoleMapper(new IdentityRoleMapper()).role("admin").permission(AuthorizationPermission.ALL);
      builder.security().authorization().enable().role("admin");
      Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {
         @Override
         public Void run() throws Exception {
            createCluster(global, builder, 2);
            waitForClusterToForm();
            return null;
         }
      });
   }

   @Override
   @AfterClass(alwaysRun = true)
   protected void destroy() {
      Security.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            ClusteredSecureCacheTest.super.destroy();
            return null;
         }
      });
   }

   @Override
   @AfterMethod(alwaysRun = true)
   protected void clearContent() throws Throwable {
      Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {
         @Override
         public Void run() throws Exception {
            try {
               ClusteredSecureCacheTest.super.clearContent();
            } catch (Throwable e) {
               throw new Exception(e);
            }
            return null;
         }
      });
   }

   public void testClusteredSecureCache() {
      Security.doAs(ADMIN, new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            Cache<String, String> cache1 = cache(0);
            Cache<String, String> cache2 = cache(1);
            cache1.put("a", "a");
            cache2.put("b", "b");
            assertEquals("a", cache2.get("a"));
            assertEquals("b", cache1.get("b"));
            return null;
         }
      });
   }
}
