package org.infinispan.server.test.client.rest;

import static org.infinispan.server.test.client.rest.RESTHelper.*;
import static org.jboss.aesh.terminal.Key.e;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Optional;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;

import org.apache.http.HttpResponse;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import sun.security.validator.ValidatorException;

@RunWith(Arquillian.class)
@Category({Security.class})
@WithRunningServer({@RunningServer(name = "restSslWithSni", config = "testsuite/rest-ssl-with-sni.xml")})
public class RESTClientWithSniEncryptionIT {

   protected static final String DEFAULT_TRUSTSTORE_PATH = ITestUtils.SERVER_CONFIG_DIR + File.separator
           + "truststore_client.jks";
   protected static final String DEFAULT_TRUSTSTORE_PASSWORD = "secret";


   @InfinispanResource("hotrodSslWithSni")
   RemoteInfinispanServer ispnServer;

   @Before
   public void setup() {
      addServer(ispnServer.getRESTEndpoint().getInetAddress().getHostName(), ispnServer.getRESTEndpoint().getContextPath());
   }

   @After
   public void release() {
      clearServers();
   }

   @Test
   public void testUnauthorizedAccessToDefaultSSLContext() throws Exception {
      //given
      SSLContext sslContext = SslContextFactory.getContext(null, null, DEFAULT_TRUSTSTORE_PATH, DEFAULT_TRUSTSTORE_PASSWORD.toCharArray());

      //when
      setSni(sslContext, Optional.empty());
      try {
         //when
         put(toSsl(fullPathKey("test")), "test", "text/plain");

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
      setSni(sslContext, Optional.of("sni"));
      HttpResponse response = put(toSsl(fullPathKey("test")), "test", "text/plain");

      //then
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
   }
}
