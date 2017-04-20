package org.infinispan.server.test.client.rest;

import static org.junit.Assert.fail;

import java.io.File;
import java.util.Optional;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpResponse;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
@Category({Security.class})
@WithRunningServer({@RunningServer(name = "restSslWithSni")})
public class RESTClientWithSniEncryptionIT {

   protected static final String DEFAULT_TRUSTSTORE_PATH = ITestUtils.SERVER_CONFIG_DIR + File.separator
           + "truststore_client.jks";
   protected static final String DEFAULT_TRUSTSTORE_PASSWORD = "secret";


   @InfinispanResource("restSslWithSni")
   RemoteInfinispanServer ispnServer;

   RESTHelper rest;

   @Before
   public void setup() {
      rest = new RESTHelper();
      rest.addServer(ispnServer.getRESTEndpoint().getInetAddress().getHostName(), ispnServer.getRESTEndpoint().getContextPath());
   }

   @After
   public void release() {
      rest.clearServers();
   }

   @Test
   public void testUnauthorizedAccessToDefaultSSLContext() throws Exception {
      //given
      SSLContext sslContext = SslContextFactory.getContext(null, null, DEFAULT_TRUSTSTORE_PATH, DEFAULT_TRUSTSTORE_PASSWORD.toCharArray());

      //when
      rest.setSni(sslContext, Optional.empty());
      try {
         //when
         rest.put(rest.toSsl(rest.fullPathKey("test")), "test", "text/plain");

         fail();
      } catch (javax.net.ssl.SSLHandshakeException ignoreMe) {
         //then
      }
   }

   @Test
   public void testAuthorizedAccessThroughSni() throws Exception {
      //given
      SSLContext sslContext = SslContextFactory.getContext(null, null, DEFAULT_TRUSTSTORE_PATH, DEFAULT_TRUSTSTORE_PASSWORD.toCharArray());

      //when
      rest.setSni(sslContext, Optional.of("sni"));
      HttpResponse response = rest.put(rest.toSsl(rest.fullPathKey("test")), "test", "text/plain");

      //then
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
   }
}
