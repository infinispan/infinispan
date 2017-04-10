package org.infinispan.rest.server;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.rest.server.authentication.ClientCertAuthenticator;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.AuthenticationTest")
public class CertificateTest extends AbstractInfinispanTest {

   public static final String TRUST_STORE_PATH = CertificateTest.class.getClassLoader().getResource("./default_client_truststore.jks").getPath();
   public static final String KEY_STORE_PATH = CertificateTest.class.getClassLoader().getResource("./default_client_truststore.jks").getPath();

   private EmbeddedCacheManager cacheManager;
   private HttpClient client;
   private RestServer restServer;

   @BeforeSuite
   public void beforeSuite() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      gcb.globalJmxStatistics().allowDuplicateDomains(true);
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cacheManager = new DefaultCacheManager(gcb.build(), cb.build());
   }

   @AfterSuite
   public void afterSuite() throws Exception {
      cacheManager.stop();
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
   public void shouldAllowProperCertificate() throws Exception {
      //given
      SslContextFactory sslContextFactory = new SslContextFactory();
      sslContextFactory.setTrustStorePassword(TRUST_STORE_PATH);
      sslContextFactory.setTrustStorePassword("secret");
      sslContextFactory.setKeyStorePath(KEY_STORE_PATH);
      sslContextFactory.setKeyStorePassword("secret");
      client = new HttpClient(sslContextFactory);
      client.start();

      RestServerConfigurationBuilder restServerConfigurationBuilder = new RestServerConfigurationBuilder();
      restServerConfigurationBuilder.port(0).host("localhost");
      restServerConfigurationBuilder
            .ssl()
            .enable()
            .keyStoreFileName(KEY_STORE_PATH)
            .keyStorePassword("secret".toCharArray())
            .trustStoreFileName(TRUST_STORE_PATH)
            .trustStorePassword("secret".toCharArray())
            .requireClientAuth(true);

      restServer = new RestServer();
      restServer.setAuthenticator(new ClientCertAuthenticator());
      restServer.start(restServerConfigurationBuilder.build(), cacheManager);

      //when
      ContentResponse response = client
            .newRequest(String.format("https://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.GET)
            .send();

      //then
      ResponseAssertion.assertThat(response).isNotFound();
   }
}
