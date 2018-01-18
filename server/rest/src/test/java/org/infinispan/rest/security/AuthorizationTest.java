package org.infinispan.rest.security;

import java.security.PrivilegedAction;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.authentication.impl.BasicAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.server.core.security.simple.SimpleServerAuthenticationProvider;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.security.AuthorizationTest")
public class AuthorizationTest extends AbstractInfinispanTest {

   public static final String REALM = "realm";
   private HttpClient client;
   private RestServerHelper restServer;

   static final Subject ADMIN;
   static final Map<AuthorizationPermission, Subject> SUBJECTS;

   static {
      // Initialize one subject per permission
      SUBJECTS = new HashMap<>(AuthorizationPermission.values().length);
      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         SUBJECTS.put(perm, TestingUtil.makeSubject(perm.toString() + "_user", perm.toString()));
      }
      ADMIN = SUBJECTS.get(AuthorizationPermission.ALL);
   }

   @BeforeSuite
   public void beforeSuite() throws Exception {
      client = new HttpClient();
      client.start();
   }

   @AfterSuite
   public void afterSuite() throws Exception {
      Security.doAs(ADMIN, (PrivilegedAction<Void>) () -> { restServer.stop(); return null; });
      client.stop();
   }

   @AfterMethod
   public void afterMethod() {
      Security.doAs(ADMIN, (PrivilegedAction<Void>) () -> { restServer.clear(); return null; });
      if (restServer != null) {
         Security.doAs(ADMIN, (PrivilegedAction<Void>) () -> { restServer.stop(); return null; });
      }
   }

   @Test
   public void testReaderCannotWrite() throws Exception {
      //given
      initRestServer();

      // when reader puts
      ContentResponse put = request("default", "key", "reader", "password", HttpMethod.PUT)
            .content(new StringContentProvider("value"))
            .send();

      //then
      ResponseAssertion.assertThat(put).isForbidden();
   }

   @Test
   public void testWriterCannotRead() throws Exception {
      //given
      initRestServer();

      // when writer gets
      ContentResponse get = request("default", "key", "writer", "password", HttpMethod.GET).send();

      //then
      ResponseAssertion.assertThat(get).isForbidden();
   }

   @Test
   public void testWriterCanWriteAndReaderCanRead() throws Exception {
      //given
      initRestServer();

      //when writer puts
      ContentResponse put = request("default", "key", "writer", "password", HttpMethod.PUT)
            .content(new StringContentProvider("value"))
            .send();

      //then
      ResponseAssertion.assertThat(put).isOk();

      // when reader gets
      ContentResponse get = request("default", "key", "reader", "password", HttpMethod.GET).send();

      //then
      ResponseAssertion.assertThat(get).isOk();
   }

   @Test
   public void testWriterCanDelete() throws Exception {
      //given
      initRestServer();

      //when writer puts
      ContentResponse put = request("default", "key", "writer", "password", HttpMethod.PUT)
            .content(new StringContentProvider("value"))
            .send();

      //then
      ResponseAssertion.assertThat(put).isOk();

      // when writer deletes
      ContentResponse delete = request("default", "key", "writer", "password", HttpMethod.DELETE).send();

      //then
      ResponseAssertion.assertThat(delete).isOk();
   }

   @Test
   public void testReaderCannotDelete() throws Exception {
      //given
      initRestServer();

      //when writer puts
      ContentResponse put = request("default", "key", "writer", "password", HttpMethod.PUT)
            .content(new StringContentProvider("value"))
            .send();

      //then
      ResponseAssertion.assertThat(put).isOk();

      // when reader deletes
      ContentResponse delete = request("default", "key", "reader", "password", HttpMethod.DELETE).send();

      //then
      ResponseAssertion.assertThat(delete).isForbidden();
   }

   private Request request(String cache, String key, String username, String password, HttpMethod method) {
      return client.newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), cache, key))
            .header(HttpHeader.AUTHORIZATION, authHeader(username, password))
            .method(method);
   }

   private String authHeader(String username, String password) {
      return "Basic " + Base64.getEncoder().encodeToString((username+":"+password).getBytes());
   }

   private void initRestServer() {
      SimpleServerAuthenticationProvider ssap = getSimpleServerAuthenticationProvider();
      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(ssap, REALM);

      restServer = Security.doAs(ADMIN, (PrivilegedAction<RestServerHelper>) () -> {
         EmbeddedCacheManager manager = getSecureCacheManager();
         RestServerHelper rest = new RestServerHelper(manager).withAuthenticator(basicAuthenticator);
         manager.defineConfiguration("default", manager.getDefaultCacheConfiguration());
         rest.start();
         return rest;
      });
   }

   private EmbeddedCacheManager getSecureCacheManager() {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalAuthConfig = global.security().authorization().enable().principalRoleMapper(new IdentityRoleMapper());
      ConfigurationBuilder config = new ConfigurationBuilder();
      AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();
      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         globalAuthConfig.role(perm.toString()).permission(perm);
         authConfig.role(perm.toString());
      }
      return TestCacheManagerFactory.createCacheManager(global, config);
   }

   private SimpleServerAuthenticationProvider getSimpleServerAuthenticationProvider() {
      SimpleServerAuthenticationProvider ssap = new SimpleServerAuthenticationProvider();
      ssap.addUser("reader", REALM, "password".toCharArray(), AuthorizationPermission.READ.name());
      ssap.addUser("writer", REALM, "password".toCharArray(), AuthorizationPermission.WRITE.name());
      ssap.addUser("admin", REALM, "password".toCharArray(),
            AuthorizationPermission.READ.name(),
            AuthorizationPermission.WRITE.name(),
            AuthorizationPermission.ADMIN.name());
      return ssap;
   }
}
