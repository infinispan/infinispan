package org.infinispan.rest;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.test.security.TestCertificates;
import org.infinispan.rest.authentication.impl.ClientCertAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
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

   @Test
   public void shouldAllowProperCertificate() throws Exception {
      restServer = RestServerHelper.defaultRestServer()
            .withAuthenticator(new ClientCertAuthenticator())
            .withKeyStore(TestCertificates.certificate("server"), TestCertificates.KEY_PASSWORD, TestCertificates.KEYSTORE_TYPE)
            .withTrustStore(TestCertificates.certificate("trust"), TestCertificates.KEY_PASSWORD, TestCertificates.KEYSTORE_TYPE)
            .withClientAuth()
            .start(TestResourceTracker.getCurrentTestShortName());

      RestClientConfigurationBuilder config = new RestClientConfigurationBuilder();
      config.security().ssl().enable()
            .trustStoreFileName(TestCertificates.certificate("ca"))
            .trustStorePassword(TestCertificates.KEY_PASSWORD)
            .trustStoreType(TestCertificates.KEYSTORE_TYPE)
            .keyStoreFileName(TestCertificates.certificate("client"))
            .keyStorePassword(TestCertificates.KEY_PASSWORD)
            .keyStoreType(TestCertificates.KEYSTORE_TYPE)
            .hostnameVerifier((hostname, session) -> true)
            .addServer().host("localhost").port(restServer.getPort());
      client = RestClient.forConfiguration(config.build());

      //when
      CompletionStage<RestResponse> response = client.raw().get("/rest/v2/caches/default/test", Collections.emptyMap());

      //then
      assertEquals(404, response.toCompletableFuture().get(10, TimeUnit.MINUTES).getStatus());
   }
}
