package org.infinispan.scripting;

import static org.testng.AssertJUnit.assertEquals;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.impl.ScriptingManagerImpl;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "scripting.SecureScriptingTest")
public class SecureScriptingTest extends ScriptingTest {

   static final Subject ADMIN = TestingUtil.makeSubject("admin", ScriptingManagerImpl.SCRIPT_MANAGER_ROLE);
   static final Subject RUNNER = TestingUtil.makeSubject("runner", "runner");

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable().principalRoleMapper(new IdentityRoleMapper());
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();

      globalRoles.role("runner").permission(AuthorizationPermission.EXEC).permission(AuthorizationPermission.READ).permission(AuthorizationPermission.WRITE).role("admin")
            .permission(AuthorizationPermission.ALL);
      authConfig.role("runner").role("admin");
      return TestCacheManagerFactory.createCacheManager(global, config);
   }

   @Override
   protected void setup() throws Exception {
      Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {
         @Override
         public Void run() throws Exception {
            SecureScriptingTest.super.setup();
            return null;
         }
      });
   }

   @Override
   protected void teardown() {
      Security.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            SecureScriptingTest.super.teardown();
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

   @Override
   @Test(expectedExceptions= { SecurityException.class, CacheException.class} )
   public void testSimpleScript() throws Exception {
      String result = (String) scriptingManager.runScript(SCRIPT_NAME).get();
      assertEquals("a", result);
   }

   public void testSimpleScriptWithEXECPermissions() throws Exception {
      String result = Security.doAs(RUNNER, new PrivilegedExceptionAction<String>() {

         @Override
         public String run() throws Exception {
            return (String) scriptingManager.runScript(SCRIPT_NAME).get();
         }
      });
      assertEquals("a", result);
   }

}
