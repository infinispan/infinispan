package org.infinispan.rest;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.rest.authentication.impl.ClientCertAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.commons.test.TestResourceTracker;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.CertificateTest")
public class CertificateTest extends AbstractInfinispanTest {

   public static final String TRUST_STORE_PATH = CertificateTest.class.getClassLoader().getResource("./client.p12").getPath();
   public static final String KEY_STORE_PATH = TRUST_STORE_PATH;

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

   @Test
   public void shouldAllowProperCertificate() throws Exception {
      restServer = RestServerHelper.defaultRestServer()
            .withAuthenticator(new ClientCertAuthenticator())
            .withKeyStore(KEY_STORE_PATH, "secret", "pkcs12")
            .withTrustStore(TRUST_STORE_PATH, "secret", "pkcs12")
            .withClientAuth()
            .start(TestResourceTracker.getCurrentTestShortName());

      RestClientConfigurationBuilder config = new RestClientConfigurationBuilder();
      config.security().ssl().enable()
            .trustStoreFileName(KEY_STORE_PATH)
            .trustStorePassword("secret".toCharArray())
            .trustStoreType("pkcs12")
            .keyStoreFileName(TRUST_STORE_PATH)
            .keyStorePassword("secret".toCharArray())
            .keyStoreType("pkcs12")
            .hostnameVerifier((hostname, session) -> true)
            .addServer().host("localhost").port(restServer.getPort());
      client = RestClient.forConfiguration(config.build());

      //when
      CompletionStage<RestResponse> response = client.raw().get("/rest/v2/caches/default/test", Collections.emptyMap());

      //then
      assertEquals(404, response.toCompletableFuture().get(10, TimeUnit.MINUTES).getStatus());
   }
}
