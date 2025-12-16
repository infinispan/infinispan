package org.infinispan.scripting;

import static org.infinispan.commons.internal.InternalCacheNames.SCRIPT_CACHE_NAME;
import static org.testng.AssertJUnit.assertEquals;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.tasks.TaskContext;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "scripting.SecureScriptingTest")
public class SecureScriptingTest extends AbstractScriptingTest {

   static final Subject ADMIN = TestingUtil.makeSubject("admin");
   static final Subject RUNNER = TestingUtil.makeSubject("runner", "runner");
   static final Subject PHEIDIPPIDES = TestingUtil.makeSubject("pheidippides", "pheidippides");
   static final Subject ACHILLES = TestingUtil.makeSubject("achilles", "achilles");
   static final String SECURE_CACHE_NAME = "secured-script-exec";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable().groupOnlyMapping(false).principalRoleMapper(new IdentityRoleMapper());
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();

      globalRoles
            .role("achilles")
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE)
            .role("runner")
            .permission(AuthorizationPermission.EXEC)
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE)
            .role("pheidippides")
            .permission(AuthorizationPermission.EXEC)
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE)
            .role("admin")
            .permission(AuthorizationPermission.ALL);
      authConfig.role("runner").role("pheidippides").role("admin");
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(global, config);
      Security.doAs(ADMIN, () -> {
         cm.defineConfiguration(ScriptingTest.CACHE_NAME, cm.getDefaultCacheConfiguration());
         cm.getCache(ScriptingTest.CACHE_NAME);
         cm.defineConfiguration(SecureScriptingTest.SECURE_CACHE_NAME, cm.getDefaultCacheConfiguration());
         cm.getCache(SecureScriptingTest.SECURE_CACHE_NAME);
         cm.defineConfiguration("nonSecuredCache", TestCacheManagerFactory.getDefaultCacheConfiguration(true).build());
      });

      return cm;
   }

   @Override
   protected String[] getScripts() {
      return new String[]{"test.js", "testRole.js", "testRoleWithCache.js"};
   }

   @Override
   protected void setup() throws Exception {
      Security.doAs(ADMIN, () -> {
         try {
            SecureScriptingTest.super.setup();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      });
   }

   @Override
   protected void teardown() {
      Security.doAs(ADMIN, () -> SecureScriptingTest.super.teardown());
   }

   @Override
   protected void clearContent() {
      Security.doAs(ADMIN, () -> cacheManager.getCache().clear());
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testSimpleScript() {
      String result = CompletionStages.join(scriptingManager.runScript("test.js", new TaskContext().addParameter("a", "a")));
      assertEquals("a", result);
   }

   public void testSimpleScriptWithEXECPermissions() {
      String result = Security.doAs(RUNNER, () -> CompletionStages.join(scriptingManager.runScript("test.js", new TaskContext().addParameter("a", "a"))));
      assertEquals("a", result);
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testSimpleScriptWithEXECPermissionsWrongRole() {
      String result = Security.doAs(RUNNER, () -> CompletionStages.join(scriptingManager.runScript("testRole.js", new TaskContext().addParameter("a", "a"))));
      assertEquals("a", result);
   }

   public void testSimpleScriptWithEXECPermissionsRightRole() {
      String result = Security.doAs(PHEIDIPPIDES, () -> CompletionStages.join(scriptingManager.runScript("testRole.js", new TaskContext().addParameter("a", "a"))));
      assertEquals("a", result);
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testSimpleScriptWithoutEXEC() {
      Security.doAs(ACHILLES, () -> CompletionStages.join(scriptingManager.runScript("testRole.js", new TaskContext().addParameter("a", "a"))));
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testUploadScriptWithEXECNotManager() {
      Security.doAs(PHEIDIPPIDES, () -> scriptingManager.addScript("my_script", "1+1"));
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testUploadScriptWithoutEXECNotManager() {
      Security.doAs(ACHILLES, () -> scriptingManager.addScript("my_script", "1+1"));
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testRemoveScriptWithEXECNotManager() {
      Security.doAs(PHEIDIPPIDES, () -> scriptingManager.removeScript("test.js"));
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testUploadScriptDirectlyWithEXECNotManager() {
      Security.doAs(PHEIDIPPIDES, () -> cacheManager.getCache(SCRIPT_CACHE_NAME).put("my_script", "1+1"));
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testRemoveScriptDirectlyWithEXECNotManager() {
      Security.doAs(PHEIDIPPIDES, () -> cacheManager.getCache(SCRIPT_CACHE_NAME).remove("test.js"));
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testClearScriptDirectlyWithEXECNotManager() {
      Security.doAs(PHEIDIPPIDES, () -> cacheManager.getCache(SCRIPT_CACHE_NAME).clear());
   }

   public void testScriptOnNonSecuredCache() {
      Cache<String, String> nonSecCache = cache("nonSecuredCache");
      nonSecCache.put("a", "value");
      assertEquals("value", nonSecCache.get("a"));

      String result = Security.doAs(PHEIDIPPIDES, () -> CompletionStages.join(scriptingManager.runScript("testRoleWithCache.js", new TaskContext().addParameter("a", "a").cache(nonSecCache))));
      assertEquals("a", result);
      assertEquals("a", nonSecCache.get("a"));
   }

   @Test(expectedExceptions = SecurityException.class)
   public void testScriptOnNonSecuredCacheWrongRole() {
         Cache<String, String> nonSecCache = cache("nonSecuredCache");
      Security.doAs(RUNNER, () -> CompletionStages.join(scriptingManager.runScript("testRoleWithCache.js", new TaskContext().addParameter("a", "a").cache(nonSecCache))));
   }
}
