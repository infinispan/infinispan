package org.infinispan.rest.security;

import java.util.Base64;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.authentication.impl.BasicAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.server.core.security.simple.SimpleServerAuthenticationProvider;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.security.BasicAuthenticationTest")
public class BasicAuthenticationTest extends AbstractInfinispanTest {

   public static final String REALM = "realm";
   private HttpClient client;
   private RestServerHelper restServer;

   @BeforeSuite
   public void beforeSuite() throws Exception {
      client = new HttpClient();
      client.start();
   }

   @AfterSuite
   public void afterSuite() throws Exception {
      restServer.stop();
      client.stop();
   }

   @AfterMethod
   public void afterMethod() throws Exception {
      restServer.clear();
      if (restServer != null) {
         restServer.stop();
      }
   }

   @Test
   public void shouldAuthenticateWhenProvidingProperCredentials() throws Exception {
      //given
      SimpleServerAuthenticationProvider ssap = getSimpleServerAuthenticationProvider();
      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(ssap, REALM);
      restServer = RestServerHelper.defaultRestServer().withAuthenticator(basicAuthenticator).start();

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.HEAD) //the method doesn't matter, we use the same tactics for all of them.
            .header(HttpHeader.AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString("user:password".getBytes()))
            .send();

      //then
      ResponseAssertion.assertThat(response).isNotFound();
   }

   private SimpleServerAuthenticationProvider getSimpleServerAuthenticationProvider() {
      SimpleServerAuthenticationProvider ssap = new SimpleServerAuthenticationProvider();
      ssap.addUser("user", REALM, "password".toCharArray());
      return ssap;
   }

   @Test
   public void shouldRejectNotValidAuthorizationString() throws Exception {
      //given
      SimpleServerAuthenticationProvider ssap = getSimpleServerAuthenticationProvider();
      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(ssap, REALM);
      restServer = RestServerHelper.defaultRestServer().withAuthenticator(basicAuthenticator).start();

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.GET)
            .header(HttpHeader.AUTHORIZATION, "Invalid string")
            .send();

      //then
      ResponseAssertion.assertThat(response).isUnauthorized();
   }

   @Test
   public void shouldRejectNoAuthentication() throws Exception {
      //given
      SimpleServerAuthenticationProvider ssap = getSimpleServerAuthenticationProvider();
      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(ssap, REALM);
      restServer = RestServerHelper.defaultRestServer().withAuthenticator(basicAuthenticator).start();

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.GET)
            .send();

      //then
      ResponseAssertion.assertThat(response).isUnauthorized();
   }
}
