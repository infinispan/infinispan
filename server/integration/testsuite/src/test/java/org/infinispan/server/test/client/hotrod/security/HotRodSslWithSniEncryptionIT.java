package org.infinispan.server.test.client.hotrod.security;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;

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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test for using SSL with SNI. The test uses 2 security realms - one for "sni" host names with proper authentication
 * details and the other one (for everything else) with no authorized hosts.
 * <p>
 *     Since this test is pretty slow (requires booting up full server with Arquillian), it contains
 *     only high level tests. For more complicated scenarios, see {@link HotRodSniFunctionalTest}.
 * </p>
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 * @see HotRodSniFunctionalTest
 */
@RunWith(Arquillian.class)
@Category({Security.class})
@WithRunningServer({@RunningServer(name = "hotrodSslWithSni", config = "testsuite/hotrod-ssl-with-sni.xml")})
public class HotRodSslWithSniEncryptionIT {

   protected static final String DEFAULT_TRUSTSTORE_PATH = ITestUtils.SERVER_CONFIG_DIR + File.separator
           + "truststore_client.jks";
   protected static final String DEFAULT_TRUSTSTORE_PASSWORD = "secret";

   protected static RemoteCache<String, String> remoteCache = null;
   protected static RemoteCacheManager remoteCacheManager = null;

   @InfinispanResource("hotrodSslWithSni")
   RemoteInfinispanServer ispnServer;

   @After
   public void release() {
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
   }

   @Test
   @Ignore
   public void testUnauthorizedAccessToDefaultSSLContext() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      String hostname = ispnServer.getHotrodEndpoint().getInetAddress().getHostName();
      builder.addServer().host(hostname).port(ispnServer.getHotrodEndpoint().getPort());
      SSLContext cont = SslContextFactory.getContext(null, null, DEFAULT_TRUSTSTORE_PATH, DEFAULT_TRUSTSTORE_PASSWORD.toCharArray());
      builder.security().ssl().sslContext(cont).enable();
      remoteCacheManager = new RemoteCacheManager(builder.build());
      try {
         remoteCacheManager.getCache(RemoteCacheManager.DEFAULT_CACHE_NAME);
      } catch (TransportException e) {
         assertTrue(e.getCause() instanceof SSLHandshakeException);
      }
   }

   @Test
   public void testAuthorizedAccessThroughSni() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      String hostname = ispnServer.getHotrodEndpoint().getInetAddress().getHostName();
      builder.addServer().host(hostname).port(ispnServer.getHotrodEndpoint().getPort());
      SSLContext cont = SslContextFactory.getContext(null, null, DEFAULT_TRUSTSTORE_PATH, DEFAULT_TRUSTSTORE_PASSWORD.toCharArray());
      builder.security().ssl()
              .sslContext(cont)
              .sniHostName("sni")
              .enable();
      remoteCacheManager = new RemoteCacheManager(builder.build());
      remoteCache = remoteCacheManager.getCache(RemoteCacheManager.DEFAULT_CACHE_NAME);
      assertNotNull(remoteCache);
   }
}
