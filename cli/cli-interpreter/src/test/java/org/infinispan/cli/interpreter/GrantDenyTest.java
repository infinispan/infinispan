package org.infinispan.cli.interpreter;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.ClusterRoleMapper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName="cli.interpreter.GrantDenyTest")
public class GrantDenyTest extends SingleCacheManagerTest {
   static final Subject ADMIN = TestingUtil.makeSubject("admin");

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();
   private ClusterRoleMapper cpm;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      TestCacheManagerFactory.configureJmx(global, getClass().getSimpleName(), mBeanServerLookup);
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable()
            .principalRoleMapper(new ClusterRoleMapper());
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();

      globalRoles
         .role("reader").permission(AuthorizationPermission.ALL_READ)
         .role("writer").permission(AuthorizationPermission.ALL_WRITE)
         .role("admin").permission(AuthorizationPermission.ALL);
      authConfig.role("reader").role("writer").role("admin").statistics().enable();
      return TestCacheManagerFactory.createCacheManager(global, config);
   }

   @Override
   protected void setup() throws Exception {
      cpm = Security.doAs(ADMIN, (PrivilegedExceptionAction<ClusterRoleMapper>) () -> {
         cacheManager = createCacheManager();
         cpm = (ClusterRoleMapper) cacheManager.getCacheManagerConfiguration().security().authorization().principalRoleMapper();
         cpm.grant("admin", "admin");
         cache = cacheManager.getCache();
         return cpm;
      });
   }

   @Override
   protected void teardown() {
      Security.doAs(ADMIN, (PrivilegedAction<Void>) () -> {
         GrantDenyTest.super.teardown();
         return null;
      });
   }

   @Override
   protected void clearContent() {
      Security.doAs(ADMIN, (PrivilegedAction<Void>) () -> {
         cacheManager.getCache().clear();
         return null;
      });
   }

   private Interpreter getInterpreter() {
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(this.cacheManager);
      return gcr.getComponent(Interpreter.class);
   }

   private Map<String, String> execute(Interpreter interpreter, String sessionId, String commands) throws Exception {
      Map<String, String> result = interpreter.execute(sessionId, commands);
      if (result.containsKey(ResultKeys.ERROR.toString())) {
         fail(String.format("%s\n%s", result.get(ResultKeys.ERROR.toString()), result.get(ResultKeys.STACKTRACE.toString())));
      }

      return result;
   }

   public void testGrantDeny() throws Exception {
      Interpreter interpreter = getInterpreter();
      String sessionId = interpreter.createSessionId(TestingUtil.extractGlobalConfiguration(cacheManager).defaultCacheName().get());
      execute(interpreter, sessionId, "grant reader to jack;");
      assertTrue(cpm.list("jack").contains("reader"));
      execute(interpreter, sessionId, "grant reader to jill;");
      assertTrue(cpm.list("jill").contains("reader"));
      execute(interpreter, sessionId, "deny reader to jack;");
      assertFalse(cpm.list("jack").contains("reader"));
      Map<String, String> response = execute(interpreter, sessionId, "roles jill;");
      assertEquals("[reader]", response.get(ResultKeys.OUTPUT.toString()));
   }
}
