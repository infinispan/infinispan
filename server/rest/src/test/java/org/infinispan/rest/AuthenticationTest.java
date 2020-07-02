package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpHeaderNames.AUTHORIZATION;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.authentication.SecurityDomain;
import org.infinispan.rest.authentication.impl.BasicAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.AuthenticationTest")
public class AuthenticationTest extends AbstractInfinispanTest {

   public static final String REALM = "ApplicationRealm";
   private static final String URL = String.format("/rest/v2/caches/%s/%s", "default", "test");
   private RestClient client;
   private RestServerHelper restServer;


   @BeforeMethod(alwaysRun = true)
   public void beforeMethod() {
      SecurityDomain securityDomainMock = mock(SecurityDomain.class);
      Subject user = TestingUtil.makeSubject("test");
      doReturn(user).when(securityDomainMock).authenticate(eq("test"), eq("test"));
      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(securityDomainMock, REALM);
      restServer = RestServerHelper.defaultRestServer().withAuthenticator(basicAuthenticator).start(TestResourceTracker.getCurrentTestShortName());

      RestClientConfigurationBuilder configurationBuilder = new RestClientConfigurationBuilder();
      configurationBuilder.addServer().host(restServer.getHost()).port(restServer.getPort());
      client = RestClient.forConfiguration(configurationBuilder.build());
   }

   @AfterMethod(alwaysRun = true)
   public void afterMethod() throws IOException {
      restServer.clear();
      if (restServer != null) {
         restServer.stop();
         client.close();
      }
   }

   @Test
   public void shouldAuthenticateWhenProvidingProperCredentials() {
      Map<String, String> headers = singletonMap(AUTHORIZATION.toString(), "Basic " + Base64.getEncoder().encodeToString("test:test".getBytes()));
      CompletionStage<RestResponse> response = client.raw().head(URL, headers);

      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void shouldRejectNotValidAuthorizationString() {
      Map<String, String> headers = new HashMap<>();
      headers.put(AUTHORIZATION.toString(), "Invalid string");

      CompletionStage<RestResponse> response = client.raw().get(URL, headers);

      ResponseAssertion.assertThat(response).isUnauthorized();
   }

   @Test
   public void shouldRejectNoAuthentication() {
      CompletionStage<RestResponse> response = client.raw().get(URL);

      //then
      ResponseAssertion.assertThat(response).isUnauthorized();
   }

   @Test
   public void shouldAllowHealthAnonymously() {
      CompletionStage<RestResponse> response = client.cacheManager("default").healthStatus();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain");
      ResponseAssertion.assertThat(response).hasReturnedText("HEALTHY");
   }
}
