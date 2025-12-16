package org.infinispan.scripting;

import static org.infinispan.scripting.utils.ScriptingUtils.getScriptingManager;
import static org.infinispan.scripting.utils.ScriptingUtils.loadScript;
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.util.List;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.tasks.TaskContext;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests verifying the script execution in secured clustered ispn environment.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "scripting.ReplicatedSecuredScriptingTest")
@CleanupAfterTest
public class ReplicatedSecuredScriptingTest extends MultipleCacheManagersTest {
   static final Subject ADMIN = TestingUtil.makeSubject("admin");
   static final Subject RUNNER = TestingUtil.makeSubject("runner", "runner");
   static final Subject PHEIDIPPIDES = TestingUtil.makeSubject("pheidippides", "pheidippides");

   @Override
   protected void createCacheManagers() throws Throwable {
      final GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      final ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC);
      global.security().authorization().enable()
            .groupOnlyMapping(false)
            .principalRoleMapper(new IdentityRoleMapper()).role("admin").permission(AuthorizationPermission.ALL)
            .role("runner")
            .permission(AuthorizationPermission.EXEC)
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE)
            .permission(AuthorizationPermission.ADMIN)
            .role("pheidippides")
            .permission(AuthorizationPermission.EXEC)
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE);
      builder.security().authorization().enable().role("admin").role("runner").role("pheidippides");
      builder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE)
            .encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      Security.doAs(ADMIN, () -> {
         createCluster(global, builder, 2);
         defineConfigurationOnAllManagers(SecureScriptingTest.SECURE_CACHE_NAME, builder);
         for (EmbeddedCacheManager cm : cacheManagers)
            cm.getCache(SecureScriptingTest.SECURE_CACHE_NAME);
         waitForClusterToForm();
      });
   }

   @Override
   @AfterClass(alwaysRun = true)
   protected void destroy() {
      Security.doAs(ADMIN, () -> ReplicatedSecuredScriptingTest.super.destroy());
   }

   @Override
   @AfterMethod(alwaysRun = true)
   protected void clearContent() throws Throwable {
      Security.doAs(ADMIN, () -> {
         try {
            ReplicatedSecuredScriptingTest.super.clearContent();
         } catch (Throwable e) {
            throw new RuntimeException(e);
         }
      });
   }

   public void testLocalScriptExecutionWithRole() {
      ScriptingManager scriptingManager = getScriptingManager(manager(0));

      Security.doAs(ADMIN, () -> {
         try {
            loadScript(scriptingManager, "/testRole.js");
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      });

      Security.doAs(PHEIDIPPIDES, () -> {
         Cache cache = manager(0).getCache(SecureScriptingTest.SECURE_CACHE_NAME);
         String value = CompletionStages.join(scriptingManager.runScript("testRole.js",
               new TaskContext().cache(cache).addParameter("a", "value")));

         assertEquals("value", value);
         assertEquals("value", cache.get("a"));
      });
   }

   @Test(expectedExceptions = {SecurityException.class})
   public void testLocalScriptExecutionWithAuthException() {
      ScriptingManager scriptingManager = getScriptingManager(manager(0));

      Security.doAs(ADMIN, () -> {
               try {
                  loadScript(scriptingManager, "/testRole.js");
               } catch (IOException e) {
                  throw new RuntimeException(e);
               }
            }
      );

      Security.doAs(RUNNER, () -> {
         Cache cache = manager(0).getCache();
         CompletionStages.join(scriptingManager.runScript("testRole.js",
               new TaskContext().cache(cache).addParameter("a", "value")));
         return null;
      });
   }

   @Test(enabled = false, description = "Enable when ISPN-6374 is fixed.")
   public void testDistributedScriptExecutionWithRole() {
      ScriptingManager scriptingManager = getScriptingManager(manager(0));

      Security.doAs(ADMIN, () -> Exceptions.unchecked(() -> loadScript(scriptingManager, "/testRole_dist.js")));

      Security.doAs(RUNNER, () -> {
         Cache cache = manager(0).getCache();
         List<Address> value = CompletionStages.join(scriptingManager.runScript("testRole_dist.js",
               new TaskContext().cache(cache).addParameter("a", "value")));

         assertEquals(value.get(0), manager(0).getAddress());
         assertEquals(value.get(1), manager(1).getAddress());
         assertEquals("value", cache.get("a"));
         assertEquals("value", manager(1).getCache().get("a"));
      });
   }

   @Test(expectedExceptions = {SecurityException.class})
   public void testDistributedScriptExecutionWithAuthException() {
      ScriptingManager scriptingManager = getScriptingManager(manager(0));

      Security.doAs(ADMIN, () -> Exceptions.unchecked(() -> loadScript(scriptingManager, "/testRole_dist.js")));

      Security.doAs(PHEIDIPPIDES, () -> {
         Cache cache = manager(0).getCache();
         CompletionStages.join(scriptingManager.runScript("testRole_dist.js",
               new TaskContext().cache(cache).addParameter("a", "value")));
      });
   }

   @DataProvider(name = "cacheModeProvider")
   private static Object[][] providePrinciples() {
      return new Object[][]{{CacheMode.REPL_SYNC}, {CacheMode.DIST_SYNC}};
   }
}
