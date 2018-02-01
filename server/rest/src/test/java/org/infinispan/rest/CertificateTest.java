package org.infinispan.rest;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.authentication.impl.ClientCertAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.AuthenticationTest")
public class CertificateTest extends AbstractInfinispanTest {

   public static final String TRUST_STORE_PATH = CertificateTest.class.getClassLoader().getResource("./default_client_truststore.jks").getPath();
   public static final String KEY_STORE_PATH = CertificateTest.class.getClassLoader().getResource("./default_client_truststore.jks").getPath();

   private HttpClient client;
   private RestServerHelper restServer;

   @AfterSuite
   public void afterSuite() throws Exception {
      restServer.stop();
   }

   @AfterMethod
   public void afterMethod() throws Exception {
      if (restServer != null) {
         restServer.stop();
      }
      client.stop();
   }

   @Test
   public void shouldAllowProperCertificate() throws Exception {
      //given
      SslContextFactory sslContextFactory = new SslContextFactory();
      sslContextFactory.setTrustStorePassword(TRUST_STORE_PATH);
      sslContextFactory.setTrustStorePassword("secret");
      sslContextFactory.setKeyStorePath(KEY_STORE_PATH);
      sslContextFactory.setKeyStorePassword("secret");

      client = new HttpClient(sslContextFactory);
      client.start();

      restServer = RestServerHelper.defaultRestServer()
            .withAuthenticator(new ClientCertAuthenticator())
            .withKeyStore(KEY_STORE_PATH, "secret")
            .withTrustStore(TRUST_STORE_PATH, "secret")
            .withClientAuth()
            .start(TestResourceTracker.getCurrentTestShortName());

      //when
      ContentResponse response = client
            .newRequest(String.format("https://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.GET)
            .send();

      //then
      ResponseAssertion.assertThat(response).isNotFound();
   }
}
