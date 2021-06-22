package org.infinispan.rest;

import static org.infinispan.rest.helper.RestServerHelper.CLIENT_KEY_STORE;
import static org.infinispan.rest.helper.RestServerHelper.SERVER_KEY_STORE;
import static org.infinispan.rest.helper.RestServerHelper.STORE_PASSWORD;
import static org.infinispan.rest.helper.RestServerHelper.STORE_TYPE;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.ssl.SslContextName;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.rest.authentication.impl.ClientCertAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.CertificateTest")
public class CertificateTest extends AbstractInfinispanTest {

   private RestClient client;
   private RestServerHelper restServer;

   @AfterSuite
   public void afterSuite() {
      restServer.stop();
   }

   @AfterMethod
   public void afterMethod() throws Exception {
      if (restServer != null) {
         restServer.stop();
      }
      client.close();
   }

   @DataProvider(name = "ssl-provider")
   public Object[][] opensslItemProvider() {
      return SslContextName.PROVIDER;
   }

   @Test(dataProvider = "ssl-provider")
   public void shouldAllowProperCertificate(String sslProvider) throws Exception {
      restServer = RestServerHelper.defaultRestServer()
            .withAuthenticator(new ClientCertAuthenticator())
            .withSslProvider(sslProvider)
            .withKeyStore(SERVER_KEY_STORE, STORE_PASSWORD, STORE_TYPE)
            .withTrustStore(SERVER_KEY_STORE, STORE_PASSWORD, STORE_TYPE)
            .withClientAuth()
            .start(TestResourceTracker.getCurrentTestShortName());

      RestClientConfigurationBuilder config = new RestClientConfigurationBuilder();
      config.security().ssl().enable()
            .trustStoreFileName(CLIENT_KEY_STORE)
            .trustStorePassword(STORE_PASSWORD)
            .trustStoreType(STORE_TYPE)
            .keyStoreFileName(CLIENT_KEY_STORE)
            .keyStorePassword(STORE_PASSWORD)
            .keyStoreType(STORE_TYPE)
            .hostnameVerifier((hostname, session) -> true)
            .addServer().host("localhost").port(restServer.getPort());
      client = RestClient.forConfiguration(config.build());

      //when
      CompletionStage<RestResponse> response = client.raw().get("/rest/v2/caches/default/test", Collections.emptyMap());

      //then
      assertEquals(404, response.toCompletableFuture().get(10, TimeUnit.MINUTES).getStatus());
   }
}
