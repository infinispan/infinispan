package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;

import javax.security.sasl.Sasl;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.security.simple.SimpleSaslAuthenticator;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.TestCallbackHandler;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.SkipException;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant
 * @since 7.0
 */
@Test(testName = "client.hotrod.AuthenticationTest", groups = "functional")
public class AuthenticationTest extends AbstractAuthenticationTest {
   private CacheMode cacheMode;

   public AuthenticationTest cacheMode(CacheMode cacheMode) {
      this.cacheMode = cacheMode;
      return this;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.clustering().cacheMode(cacheMode);
      if (cacheMode.isClustered()) {
         cacheManager = TestCacheManagerFactory.createClusteredCacheManager(builder);
      } else {
         cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      }
      cacheManager.getCache();

      hotrodServer = initServer(Collections.emptyMap(), 0);

      return cacheManager;
   }

   @Factory
   public Object[] factory() {
      return new Object[]{
            new AuthenticationTest().cacheMode(CacheMode.LOCAL),
            new AuthenticationTest().cacheMode(CacheMode.DIST_SYNC),
      };
   }

   @Override
   protected String parameters() {
      return "[" + cacheMode + "]";
   }

   @Override
   protected SimpleSaslAuthenticator createAuthenticationProvider() {
      SimpleSaslAuthenticator sap = new SimpleSaslAuthenticator();
      sap.addUser("user", "realm", "password".toCharArray());
      return sap;
   }

   @Test
   public void testAuthentication() {
      ConfigurationBuilder clientBuilder = newClientBuilder();
      clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("user", "realm", "password"));
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      RemoteCache<String, String> defaultRemote = remoteCacheManager.getCache();
      defaultRemote.put("a", "a");
      assertEquals("a", defaultRemote.get("a"));
   }

   @Test
   public void testAuthenticationViaURI() {
      remoteCacheManager = new RemoteCacheManager("hotrod://user:password@localhost:" + hotrodServer.getPort() + "?auth_realm=realm&socket_timeout=3000&max_retries=3&connection_pool.max_active=1&sasl_mechanism=CRAM-MD5&default_executor_factory.threadname_prefix=" + TestResourceTracker.getCurrentTestShortName() + "-Client-Async");
      RemoteCache<String, String> defaultRemote = remoteCacheManager.getCache();
      defaultRemote.put("a", "a");
      assertEquals("a", defaultRemote.get("a"));
   }

   @Test
   public void testAuthenticationWithUnsupportedMech() {
      ConfigurationBuilder clientBuilder = newClientBuilder();
      clientBuilder.security().authentication().saslMechanism("SCRAM-SHA-256");
      clientBuilder.security().authentication().username("user").password("password");
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      Exceptions.expectException(TransportException.class, SecurityException.class, ".*not among the supported server mechanisms.*",
            () -> {
               RemoteCache<String, String> defaultRemote = remoteCacheManager.getCache();
               defaultRemote.put("a", "a");
               assertEquals("a", defaultRemote.get("a"));
            });
   }

   @Test
   public void testAuthenticationFailWrongAuth() {
      ConfigurationBuilder clientBuilder = newClientBuilder();
      clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("user", "realm", "foobar"));
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      Exceptions.expectException(TransportException.class, HotRodClientException.class, ".*Invalid response.*", remoteCacheManager::getCache);
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN006017:.*")
   public void testAuthenticationFailNoAuth() {
      if (cacheMode.isClustered()) {
         // Test doesn't work with clustered as it registers a HotRodServer on the same cacheManager
         throw new SkipException("Test only supports local mode");
      }
      HotRodServer noAnonymousServer = initServer(Collections.singletonMap(Sasl.POLICY_NOANONYMOUS, "true"), 1);
      try {
         ConfigurationBuilder clientBuilder = newClientBuilder(1);
         clientBuilder.security().authentication().disable();
         remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
         RemoteCache<String, String> cache = remoteCacheManager.getCache();
         cache.put("a", "a");
      } finally {
         ServerTestingUtil.killServer(noAnonymousServer);
      }
   }

   @Test
   public void testAuthenticationUsername() {
      ConfigurationBuilder clientBuilder = newClientBuilder();
      clientBuilder.security().authentication().username("user").realm("realm").password("password");
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      RemoteCache<String, String> defaultRemote = remoteCacheManager.getCache();
      defaultRemote.put("a", "a");
      assertEquals("a", defaultRemote.get("a"));
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = ".*ISPN004067.*")
   public void testAuthenticationUsernameWithCallbackFail() {
      ConfigurationBuilder clientBuilder = newClientBuilder();
      clientBuilder.security().authentication().username("user").realm("realm").password("password")
                   .callbackHandler(new TestCallbackHandler("user", "realm", "foobar"));
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
   }

}
