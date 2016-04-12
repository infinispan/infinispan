package org.infinispan.server.test.client.hotrod.security;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import javax.net.ssl.SSLContext;

import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutGet;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testSize;

/**
 * Test for using SSL for client server communication encryption.
 *
 * @author vjuranek
 * @since 9.0
 */
@RunWith(Arquillian.class)
@Category({Security.class})
@WithRunningServer({@RunningServer(name = "hotrodSslNoAuth", config = "testsuite/hotrod-ssl-no-auth.xml")})
public class HotRodSslEncryptionIT {

   protected static final String DEFAULT_TRUSTSTORE_PATH = ITestUtils.SERVER_CONFIG_DIR + File.separator
           + "truststore_client.jks";
   protected static final String DEFAULT_TRUSTSTORE_PASSWORD = "secret";

   protected static RemoteCache<String, String> remoteCache = null;
   protected static RemoteCacheManager remoteCacheManager = null;

   @InfinispanResource("hotrodSslNoAuth")
   RemoteInfinispanServer ispnServer;

   @After
   public void release() {
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
   }

   @Test
   public void testViaDirectConfig() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      String hostname = ispnServer.getHotrodEndpoint().getInetAddress().getHostName();
      builder.addServer().host(hostname).port(ispnServer.getHotrodEndpoint().getPort());
      builder.security().ssl().enable().trustStoreFileName(DEFAULT_TRUSTSTORE_PATH).trustStorePassword(DEFAULT_TRUSTSTORE_PASSWORD.toCharArray());
      remoteCacheManager = new RemoteCacheManager(builder.build());
      remoteCache = remoteCacheManager.getCache(RemoteCacheManager.DEFAULT_CACHE_NAME);
      testPutGet(remoteCache);
      testSize(remoteCache);
   }

   @Test
   public void testViaSslContextSetup() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      String hostname = ispnServer.getHotrodEndpoint().getInetAddress().getHostName();
      builder.addServer().host(hostname).port(ispnServer.getHotrodEndpoint().getPort());
      SSLContext cont = SslContextFactory.getContext(null, null, DEFAULT_TRUSTSTORE_PATH, DEFAULT_TRUSTSTORE_PASSWORD.toCharArray());
      builder.security().ssl().sslContext(cont).enable();
      remoteCacheManager = new RemoteCacheManager(builder.build());
      remoteCache = remoteCacheManager.getCache(RemoteCacheManager.DEFAULT_CACHE_NAME);
      testPutGet(remoteCache);
      testSize(remoteCache);
   }
}
