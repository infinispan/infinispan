package org.infinispan.rest;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Base64;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.security.auth.Subject;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.authentication.SecurityDomain;
import org.infinispan.rest.authentication.impl.BasicAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.commons.test.TestResourceTracker;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.AuthenticationTest")
public class AuthenticationTest extends AbstractInfinispanTest {

   private HttpClient client;
   private RestServerHelper restServer;

   @BeforeClass
   public void beforeSuite() throws Exception {
      client = new HttpClient();
      client.start();
   }

   @AfterClass(alwaysRun = true)
   public void afterSuite() throws Exception {
      client.stop();
   }

   @AfterMethod(alwaysRun = true)
   public void afterMethod() {
      restServer.clear();
      if (restServer != null) {
         restServer.stop();
      }
   }

   @Test
   public void shouldAuthenticateWhenProvidingProperCredentials() throws Exception {
      //given
      SecurityDomain securityDomainMock = mock(SecurityDomain.class);
      Subject user = TestingUtil.makeSubject("test");
      doReturn(user).when(securityDomainMock).authenticate(eq("test"), eq("test"));

      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(securityDomainMock, "ApplicationRealm");
      restServer = RestServerHelper.defaultRestServer().withAuthenticator(basicAuthenticator).start(TestResourceTracker.getCurrentTestShortName());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.HEAD) //the method doesn't matter, we use the same tectics for all of them.
            .header(HttpHeader.AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString("test:test".getBytes()))
            .send();

      //then
      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void shouldRejectNotValidAuthorizationString() throws Exception {
      //given
      SecurityDomain securityDomainMock = mock(SecurityDomain.class);

      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(securityDomainMock, "ApplicationRealm");

      restServer = RestServerHelper.defaultRestServer().withAuthenticator(basicAuthenticator).start(TestResourceTracker.getCurrentTestShortName());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.GET)
            .header(HttpHeader.AUTHORIZATION, "Invalid string")
            .send();

      //then
      ResponseAssertion.assertThat(response).isUnauthorized();
   }

   @Test
   public void shouldRejectNoAuthentication() throws Exception {
      //given
      SecurityDomain securityDomainMock = mock(SecurityDomain.class);

      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(securityDomainMock, "ApplicationRealm");

      restServer = RestServerHelper.defaultRestServer().withAuthenticator(basicAuthenticator).start(TestResourceTracker.getCurrentTestShortName());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.GET)
            .send();

      //then
      ResponseAssertion.assertThat(response).isUnauthorized();
   }

   @Test
   public void shouldAllowHealthAnonymously() throws InterruptedException, ExecutionException, TimeoutException {
      SecurityDomain securityDomainMock = mock(SecurityDomain.class);
      Subject user = TestingUtil.makeSubject("test");
      doReturn(user).when(securityDomainMock).authenticate(eq("test"), eq("test"));
      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(securityDomainMock, "ApplicationRealm");

      restServer = RestServerHelper.defaultRestServer().withAuthenticator(basicAuthenticator).start(TestResourceTracker.getCurrentTestShortName());

      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/cache-managers/DefaultCacheManager/health/status", restServer.getPort()))
            .method(HttpMethod.GET)
            .send();

      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain");
      ResponseAssertion.assertThat(response).hasReturnedText("HEALTHY");
   }
}
