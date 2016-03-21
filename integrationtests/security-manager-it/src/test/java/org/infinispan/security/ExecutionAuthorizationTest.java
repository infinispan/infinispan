package org.infinispan.security;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import javax.security.auth.Subject;
import java.io.Serializable;
import java.security.Policy;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static org.testng.AssertJUnit.assertEquals;

/**
 * ExecutionAuthorizationTest.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Test(groups = "functional", testName = "security.ExecutionAuthorizationTest")
public class ExecutionAuthorizationTest extends MultipleCacheManagersTest {
   private static final String EXECUTION_CACHE = "executioncache";
   Subject ADMIN = TestingUtil.makeSubject("admin");
   Subject EXEC = TestingUtil.makeSubject("exec");
   Subject NOEXEC = TestingUtil.makeSubject("noexec");



   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.security().authorization().enable().role("admin").role("exec").role("noexec");
      Subject.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            addClusterEnabledCacheManager(getSecureClusteredGlobalConfiguration(), builder);
            addClusterEnabledCacheManager(getSecureClusteredGlobalConfiguration(), builder);
            for (EmbeddedCacheManager cm : cacheManagers) {
               cm.defineConfiguration(EXECUTION_CACHE, builder.build());
               cm.getCache(EXECUTION_CACHE);
            }
            waitForClusterToForm(EXECUTION_CACHE);
            return null;
         }
      });
   }

   private GlobalConfigurationBuilder getSecureClusteredGlobalConfiguration() {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.security().authorization()
         .enable()
         .principalRoleMapper(new IdentityRoleMapper())
         .role("admin")
            .permission(AuthorizationPermission.ALL)
         .role("exec")
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE)
            .permission(AuthorizationPermission.EXEC)
         .role("noexec")
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE);
      return global;
   }

   @Override
   @AfterClass(alwaysRun = true)
   protected void destroy() {
      Subject.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            ExecutionAuthorizationTest.super.destroy();
            return null;
         }
      });
   }

   @Override
   @AfterClass(alwaysRun = true)
   protected void clearContent() throws Exception {
      Subject.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {

         @Override
         public Void run() throws Exception {
            try {
               ExecutionAuthorizationTest.super.clearContent();
            } catch (Throwable e) {
               throw new Exception(e);
            }
            return null;
         }

      });
   }

   private void distExecTest() throws Exception {
      DefaultExecutorService des = new DefaultExecutorService(cache(0, EXECUTION_CACHE));
      CompletableFuture<Integer> future = des.submit(new SimpleCallable());
      assertEquals(Integer.valueOf(1), future.get());
   }

   public void testExecDistExec() throws Exception {
      Policy.setPolicy(new SurefireTestingPolicy());
      System.setSecurityManager(new SecurityManager());
      try {
         Subject.doAs(EXEC, new PrivilegedExceptionAction<Void>() {

            @Override
            public Void run() throws Exception {
               distExecTest();
               return null;
            }
         });
      } finally {
         System.setSecurityManager(null);
         Policy.setPolicy(null);
      }
   }

   @Test(expectedExceptions=SecurityException.class)
   public void testNoExecDistExec() throws Exception {
      Policy.setPolicy(new SurefireTestingPolicy());
      try {
         System.setSecurityManager(new SecurityManager());
         Subject.doAs(NOEXEC, new PrivilegedExceptionAction<Void>() {

            @Override
            public Void run() throws Exception {
               distExecTest();
               return null;
            }
         });
      } finally {
         System.setSecurityManager(null);
         Policy.setPolicy(null);
      }
   }


   static class SimpleCallable implements Callable<Integer>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = -8589149500259272402L;

      public SimpleCallable() {
      }

      @Override
      public Integer call() throws Exception {
         return 1;
      }
   }
}
