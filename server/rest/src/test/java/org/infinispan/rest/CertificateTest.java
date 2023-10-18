package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.Queue;

import org.assertj.core.api.Assertions;
import org.infinispan.rest.authentication.impl.ClientCertAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.rest.http2.NettyHttpClient;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

@Test(groups = "functional", testName = "rest.CertificateTest")
public class CertificateTest extends AbstractInfinispanTest {

   public static final String TRUST_STORE_PATH = CertificateTest.class.getClassLoader().getResource("./client.p12").getPath();
   public static final String KEY_STORE_PATH = CertificateTest.class.getClassLoader().getResource("./client.p12").getPath();

   private NettyHttpClient client;
   private RestServerHelper restServer;

   @AfterSuite
   public void afterSuite() {
      restServer.stop();
   }

   @AfterMethod
   public void afterMethod() {
      if (restServer != null) {
         restServer.stop();
      }
      client.stop();
   }

   @Test
   public void shouldAllowProperCertificate() throws Exception {
      //given
      restServer = RestServerHelper.defaultRestServer()
            .withAuthenticator(new ClientCertAuthenticator())
            .withKeyStore(KEY_STORE_PATH, "secret", "pkcs12")
            .withTrustStore(TRUST_STORE_PATH, "secret", "pkcs12")
            .withClientAuth()
            .start(TestResourceTracker.getCurrentTestShortName());
      client = NettyHttpClient.newHttp2ClientWithALPN(KEY_STORE_PATH, "secret");
      client.start(restServer.getHost(), restServer.getPort());

      //when
      FullHttpRequest get = new DefaultFullHttpRequest(HTTP_1_1, GET, "/rest/default/test");
      client.sendRequest(get);
      Queue<FullHttpResponse> responses = client.getResponses();

      //then
      Assertions.assertThat(responses).hasSize(1);
      Assertions.assertThat(responses.element().status().code()).isEqualTo(404);
   }
}
