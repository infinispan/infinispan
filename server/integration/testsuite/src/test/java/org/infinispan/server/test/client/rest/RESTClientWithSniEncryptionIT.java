package org.infinispan.server.test.client.rest;

import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpResponse;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.category.SingleNode;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.server.test.util.ManagementClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.wildfly.extras.creaper.commands.security.realms.AddSecurityRealm;

@RunWith(Arquillian.class)
@Category({SingleNode.class})
public class RESTClientWithSniEncryptionIT {

   protected static final String DEFAULT_TRUSTSTORE_PATH = ITestUtils.SERVER_CONFIG_DIR + File.separator
           + "truststore_client.jks";
   protected static final String DEFAULT_TRUSTSTORE_PASSWORD = "secret";
   private static final String CACHE_TEMPLATE = "localCacheConfiguration";
   private static final String CACHE_CONTAINER = "local";
   private static final String REST_ENDPOINT = "rest-connector2";
   private static final String REST_NAMED_CACHE = "restNamedCache";
   private static final int REST_PORT = 8081;

   @InfinispanResource("container1")
   RemoteInfinispanServer ispnServer;

   RESTHelper rest;

   @BeforeClass
   public static void beforeClass() throws Exception {
      ManagementClient client = ManagementClient.getStandaloneInstance();
      client.addCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
      client.addCache(REST_NAMED_CACHE, CACHE_CONTAINER, CACHE_TEMPLATE, ManagementClient.CacheType.LOCAL);
      client.addSecurityRealm("SSLRealm1");
      client.addServerIdentity("SSLRealm1", "keystore_client.jks", "jboss.server.config.dir", "secret");
      client.addSecurityRealm("SSLRealm2");
      client.addServerIdentity("SSLRealm2", "keystore_server.jks", "jboss.server.config.dir", "secret");
      client.reloadIfRequired();
      Map<String, String> vaultOptions = new HashMap<>();
      vaultOptions.put("KEYSTORE_URL", "${jboss.server.config.dir}/vault/vault.keystore");
      vaultOptions.put("KEYSTORE_PASSWORD", "MASK-AI3ZRwVO1Pd");
      vaultOptions.put("KEYSTORE_ALIAS", "ispn-vault");
      vaultOptions.put("SALT", "12345678");
      vaultOptions.put("ITERATION_COUNT", "23");
      vaultOptions.put("ENC_FILE_DIR", "${jboss.server.config.dir}/vault/");
      client.addVault(vaultOptions);
      client.addSocketBinding("rest2", "standard-sockets", REST_PORT);
      client.addRestEndpoint(REST_ENDPOINT, CACHE_CONTAINER, REST_NAMED_CACHE, "rest2");
      client.addRestEncryption(REST_ENDPOINT, "SSLRealm1", false);
      client.addRestEncryptionSNI(REST_ENDPOINT, "sni", "sni", "SSLRealm2");
      client.addRestEncryptionSNI(REST_ENDPOINT, "sni2", "sni2", null);
      client.reloadIfRequired();
   }

   @AfterClass
   public static void afterClass() throws Exception {
      ManagementClient client = ManagementClient.getStandaloneInstance();
      client.removeRestEndpoint(REST_ENDPOINT);
      client.removeSocketBinding("rest2", "standard-sockets");
      client.removeCache(REST_NAMED_CACHE, CACHE_CONTAINER, ManagementClient.CacheType.LOCAL);
      client.removeCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
      client.removeVault();
      client.removeSecurityRealm("SSLRealm1");
      client.removeSecurityRealm("SSLRealm2");
   }

   @Before
   public void setup() {
      ispnServer.reconnect();
      rest = new RESTHelper();
      rest.addServer(ispnServer.getRESTEndpoint().getInetAddress().getHostName(), REST_PORT, ispnServer.getRESTEndpoint().getContextPath());
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
