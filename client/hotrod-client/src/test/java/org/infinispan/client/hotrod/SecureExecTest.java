package org.infinispan.client.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.commons.test.CommonsTestingUtil.loadFileAsString;
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.server.core.security.simple.SimpleServerAuthenticationProvider;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.TestCallbackHandler;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests verifying script execution over HotRod Client with enabled authentication.
 *
 * @author Anna Manukyan
 */
@Test(testName = "client.hotrod.SecureExecTest", groups = "functional")
public class SecureExecTest extends AbstractAuthenticationTest {
   static final Subject ADMIN = TestingUtil.makeSubject("admin", ScriptingManager.SCRIPT_MANAGER_ROLE);
   static final String CACHE_NAME = "secured-exec";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable().principalRoleMapper(new IdentityRoleMapper());
      globalRoles
            .role("admin")
            .permission(AuthorizationPermission.ALL)
            .role("RWEuser")
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE)
            .permission(AuthorizationPermission.EXEC)
            .role("RWuser")
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE);

      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config
            .security().authorization().enable().role("admin").role("RWEuser").role("RWuser");
      config.encoding().key().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      config.encoding().value().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      cacheManager = TestCacheManagerFactory.createCacheManager(global, config);
      cacheManager.defineConfiguration(CACHE_NAME, config.build());
      cacheManager.getCache();

      hotrodServer = initServer(Collections.emptyMap(), 0);

      return cacheManager;
   }

   @Override
   protected SimpleServerAuthenticationProvider createAuthenticationProvider() {
      SimpleServerAuthenticationProvider sap = new SimpleServerAuthenticationProvider();
      sap.addUser("RWEuser", "realm", "password".toCharArray(), null);
      sap.addUser("RWuser", "realm", "password".toCharArray(), null);
      return sap;
   }

   @Override
   protected void setup() throws Exception {
      Security.doAs(ADMIN, (PrivilegedExceptionAction<Void>) () -> {
         SecureExecTest.super.setup();
         return null;
      });
   }

   @Override
   protected void teardown() {
      Security.doAs(ADMIN, (PrivilegedAction<Void>) () -> {
         SecureExecTest.super.teardown();
         return null;
      });
   }

   @Override
   protected void clearCacheManager() {
      Security.doAs(ADMIN, () -> cacheManager.getCache().clear());

      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;
   }

   @Override
   protected HotRodServer initServer(Map<String, String> mechProperties, int index) {
      return Security.doAs(ADMIN, (PrivilegedAction<HotRodServer>) () -> SecureExecTest.super.initServer(mechProperties, index));
   }

   public void testSimpleScriptExecutionWithValidAuth() throws IOException, PrivilegedActionException {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = newClientBuilder();
      clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("RWEuser", "realm", "password".toCharArray()));

      runTestWithGivenScript(clientBuilder.build(), "/testRole_hotrod.js");
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*Unauthorized access.*")
   public void testSimpleScriptExecutionWithInvalidAuth() throws IOException, PrivilegedActionException {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = newClientBuilder();
      clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("RWEuser", "realm", "password".toCharArray()));

      runTestWithGivenScript(clientBuilder.build(), "/testRole.js");
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*Unauthorized access.*")
   public void testSimpleScriptExecutionWithoutExecPerm() throws IOException, PrivilegedActionException {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = newClientBuilder();
      clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("RWuser", "realm", "password".toCharArray()));

      runTestWithGivenScript(clientBuilder.build(), "/testWithoutRole.js");
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*Unauthorized access.*")
   public void testUploadWithoutScriptManagerRole() throws IOException, PrivilegedActionException {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = newClientBuilder();
      clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("RWEuser", "realm", "password".toCharArray()));

      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCacheManager.getCache(ScriptingManager.SCRIPT_CACHE).put("shouldFail", "1+1");
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*Unauthorized access.*")
   public void testClearWithoutScriptManagerRole() throws IOException, PrivilegedActionException {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = newClientBuilder();
      clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("RWEuser", "realm", "password".toCharArray()));

      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCacheManager.getCache(ScriptingManager.SCRIPT_CACHE).clear();
   }

   private void runTestWithGivenScript(Configuration config, String scriptPath) throws IOException, PrivilegedActionException {
      remoteCacheManager = new RemoteCacheManager(config);
      Map<String, String> params = new HashMap<>();
      params.put("a", "guinness");

      String scriptName = null;
      try (InputStream is = this.getClass().getResourceAsStream(scriptPath)) {
         String script = loadFileAsString(is);

         scriptName = scriptPath.substring(1);
         uploadScript(scriptName, script);
      }

      String result = remoteCacheManager.getCache(CACHE_NAME).execute(scriptName, params);
      assertEquals("guinness", result);
      assertEquals("guinness", remoteCacheManager.getCache(CACHE_NAME).get("a"));
   }

   protected void uploadScript(String scriptName, String script) throws PrivilegedActionException {
      Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {
         @Override
         public Void run() throws Exception {
            cacheManager.getCache(ScriptingManager.SCRIPT_CACHE).put(scriptName, script);
            return null;
         }
      });
   }

}
