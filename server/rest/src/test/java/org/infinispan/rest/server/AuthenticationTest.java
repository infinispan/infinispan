package org.infinispan.rest.server;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.security.Principal;
import java.util.Base64;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.rest.server.authentication.Authenticator;
import org.infinispan.rest.server.authentication.BasicAuthenticator;
import org.infinispan.test.AbstractInfinispanTest;
import org.jboss.resteasy.plugins.server.embedded.SecurityDomain;
import org.mockito.internal.stubbing.answers.ThrowsExceptionClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.AuthenticationTest")
public class AuthenticationTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;
   private HttpClient client;
   private RestServer restServer;
   private RestServerConfigurationBuilder restServerConfiguration;

   @BeforeSuite
   public void beforeSuite() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      gcb.globalJmxStatistics().allowDuplicateDomains(true);
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cacheManager = new DefaultCacheManager(gcb.build(), cb.build());

      restServerConfiguration = new RestServerConfigurationBuilder();
      restServerConfiguration.host("localhost").port(0);
   }

   @AfterSuite
   public void afterSuite() throws Exception {
      cacheManager.stop();
   }

   @BeforeMethod
   public void beforeMethod() throws Exception {
      client = new HttpClient();
      client.start();
   }

   @AfterMethod
   public void afterMethod() throws Exception {
      cacheManager.getCache("default").clear();
      if (restServer != null) {
         restServer.stop();
      }
      client.stop();
   }

   @Test
   public void shouldAuthenticateWhenProvidingProperCredentials() throws Exception {
      //given
      SecurityDomain securityDomainMock = mock(SecurityDomain.class, new ThrowsExceptionClass(SecurityException.class));
      doReturn(mock(Principal.class)).when(securityDomainMock).authenticate(eq("test"), eq("test"));

      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(securityDomainMock, "ApplicationRealm");

      startRestWithAuthenticator(basicAuthenticator);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.GET)
            .header(HttpHeader.AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString("test:test".getBytes()))
            .send();

      //then
      ResponseAssertion.assertThat(response).isNotFound();
   }

   private void startRestWithAuthenticator(Authenticator basicAuthenticator) {
      restServer = new RestServer();
      restServer.setAuthenticator(basicAuthenticator);
      restServer.start(restServerConfiguration.build(), cacheManager);
   }

   @Test
   public void shouldRejectNotValidAuthorizationString() throws Exception {
      //given
      SecurityDomain securityDomainMock = mock(SecurityDomain.class);

      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(securityDomainMock, "ApplicationRealm");

      startRestWithAuthenticator(basicAuthenticator);

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
      SecurityDomain securityDomainMock = mock(SecurityDomain.class);

      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(securityDomainMock, "ApplicationRealm");

      startRestWithAuthenticator(basicAuthenticator);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.GET)
            .send();

      //then
      ResponseAssertion.assertThat(response).isUnauthorized();
   }
}
