package org.infinispan.server.test.client.hotrod.security;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLHandshakeException;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.category.SingleNode;
import org.infinispan.server.test.util.ManagementClient;
import org.infinispan.server.test.util.security.SecurityConfigurationHelper;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
@Category({SingleNode.class})
public class HotRodSslWithSniEncryptionIT {

   private static final String CACHE_TEMPLATE = "localCacheConfiguration";
   private static final String CACHE_CONTAINER = "local";
   private static final String HOTROD_ENDPOINT = "hotrod-connector2";
   private static final String HOTROD_NAMED_CACHE = "hotRodNamedCache";
   private static final int HOTROD_PORT = 11223;

   protected static RemoteCache<String, String> remoteCache = null;
   protected static RemoteCacheManager remoteCacheManager = null;

   @InfinispanResource("container1")
   RemoteInfinispanServer ispnServer;

   @BeforeClass
   public static void beforeClass() throws Exception {
      ManagementClient client = ManagementClient.getStandaloneInstance();
      client.addCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
      client.addCache(HOTROD_NAMED_CACHE, CACHE_CONTAINER, CACHE_TEMPLATE, ManagementClient.CacheType.LOCAL);
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
      client.addSocketBinding("hotrod2", "standard-sockets", HOTROD_PORT);
      client.addHotRodEndpoint(HOTROD_ENDPOINT, CACHE_CONTAINER, HOTROD_NAMED_CACHE, "hotrod2");
      client.addHotRodEncryption(HOTROD_ENDPOINT, "SSLRealm1", false);
      client.addHotRodEncryptionSNI(HOTROD_ENDPOINT, "sni", "sni", "SSLRealm2");
      client.addHotRodEncryptionSNI(HOTROD_ENDPOINT, "sni2", "sni2", null);
      client.reloadIfRequired();
   }

   @AfterClass
   public static void afterClass() throws Exception {
      ManagementClient client = ManagementClient.getStandaloneInstance();
      client.removeHotRodEndpoint(HOTROD_ENDPOINT);
      client.removeSocketBinding("hotrod2", "standard-sockets");
      client.removeCache(HOTROD_NAMED_CACHE, CACHE_CONTAINER, ManagementClient.CacheType.LOCAL);
      client.removeCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
      client.removeVault();
      client.removeSecurityRealm("SSLRealm1");
      client.removeSecurityRealm("SSLRealm2");
   }

   @After
   public void release() {
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
   }

   @Test
   public void testUnauthorizedAccessToDefaultSSLContext() throws Exception {
      ConfigurationBuilder builder = new SecurityConfigurationHelper().withDefaultSsl();
      String hostname = ispnServer.getHotrodEndpoint().getInetAddress().getHostName();
      builder.addServer().host(hostname).port(HOTROD_PORT);
      remoteCacheManager = new RemoteCacheManager(builder.build());
      try {
         remoteCacheManager.getCache(HOTROD_NAMED_CACHE);
      } catch (TransportException e) {
         assertTrue(e.getCause() instanceof SSLHandshakeException);
      }
   }

   @Test
   public void testAuthorizedAccessThroughSni() throws Exception {
      ConfigurationBuilder builder = new SecurityConfigurationHelper().withDefaultSsl().withSni("sni");
      String hostname = ispnServer.getHotrodEndpoint().getInetAddress().getHostName();
      builder.addServer().host(hostname).port(HOTROD_PORT);
      remoteCacheManager = new RemoteCacheManager(builder.build());
      remoteCache = remoteCacheManager.getCache(HOTROD_NAMED_CACHE);
      assertNotNull(remoteCache);
   }
}
