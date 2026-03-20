package org.infinispan.rest;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.rest.authentication.impl.ClientCertAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.testing.TestResourceTracker;
import org.infinispan.testing.security.TestCertificates;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.PemCertificateTest")
public class PemCertificateTest extends AbstractInfinispanTest {

   private RestClient client;
   private RestServerHelper restServer;

   @AfterMethod
   public void afterMethod() throws Exception {
      if (client != null) {
         client.close();
      }
      if (restServer != null) {
         restServer.stop();
      }
   }

   @Test
   public void shouldConnectWithPkcs12TrustStore() throws Exception {
      restServer = createServer();

      RestClientConfigurationBuilder config = new RestClientConfigurationBuilder();
      config.security().ssl().enable()
            .trustStoreFileName(TestCertificates.certificate("ca"))
            .trustStorePassword(TestCertificates.KEY_PASSWORD)
            .trustStoreType(TestCertificates.KEYSTORE_TYPE)
            .keyStoreFileName(TestCertificates.certificate("client"))
            .keyStorePassword(TestCertificates.KEY_PASSWORD)
            .keyStoreType(TestCertificates.KEYSTORE_TYPE)
            .hostnameVerifier((hostname, session) -> true)
            .addServer().host("localhost").port(restServer.getPort()).protocol(Protocol.HTTP_11);
      client = RestClient.forConfiguration(config.build());

      CompletionStage<RestResponse> response = client.raw().get("/rest/v2/caches/default/test", Collections.emptyMap());
      assertEquals(404, response.toCompletableFuture().get(10, TimeUnit.MINUTES).status());
   }

   @Test
   public void shouldConnectWithPemTrustStore() throws Exception {
      restServer = createServer();

      RestClientConfigurationBuilder config = new RestClientConfigurationBuilder();
      config.security().ssl().enable()
            .trustStoreFileName(TestCertificates.pem("trust"))
            .keyStoreFileName(TestCertificates.certificate("client"))
            .keyStorePassword(TestCertificates.KEY_PASSWORD)
            .keyStoreType(TestCertificates.KEYSTORE_TYPE)
            .hostnameVerifier((hostname, session) -> true)
            .addServer().host("localhost").port(restServer.getPort()).protocol(Protocol.HTTP_11);
      client = RestClient.forConfiguration(config.build());

      CompletionStage<RestResponse> response = client.raw().get("/rest/v2/caches/default/test", Collections.emptyMap());
      assertEquals(404, response.toCompletableFuture().get(10, TimeUnit.MINUTES).status());
   }

   @Test
   public void shouldConnectWithPemKeyStore() throws Exception {
      restServer = createServer();

      RestClientConfigurationBuilder config = new RestClientConfigurationBuilder();
      config.security().ssl().enable()
            .trustStoreFileName(TestCertificates.pem("trust"))
            .keyStoreFileName(TestCertificates.pem("client"))
            .keyStorePassword(TestCertificates.KEY_PASSWORD)
            .hostnameVerifier((hostname, session) -> true)
            .addServer().host("localhost").port(restServer.getPort()).protocol(Protocol.HTTP_11);
      client = RestClient.forConfiguration(config.build());

      CompletionStage<RestResponse> response = client.raw().get("/rest/v2/caches/default/test", Collections.emptyMap());
      assertEquals(404, response.toCompletableFuture().get(10, TimeUnit.MINUTES).status());
   }

   private RestServerHelper createServer() {
      return RestServerHelper.defaultRestServer()
            .withAuthenticator(new ClientCertAuthenticator())
            .withKeyStore(TestCertificates.certificate("server"), TestCertificates.KEY_PASSWORD, TestCertificates.KEYSTORE_TYPE)
            .withTrustStore(TestCertificates.certificate("trust"), TestCertificates.KEY_PASSWORD, TestCertificates.KEYSTORE_TYPE)
            .withClientAuth()
            .start(TestResourceTracker.getCurrentTestShortName());
   }
}
