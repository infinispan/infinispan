package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;

import javax.security.sasl.Sasl;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.security.simple.SimpleServerAuthenticationProvider;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.TestCallbackHandler;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant
 * @since 7.0
 */
@Test(testName = "client.hotrod.AuthenticationTest", groups = "functional")
@CleanupAfterMethod
public class AuthenticationTest extends AbstractAuthenticationTest {
   protected HotRodServer hotrodServer;


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cacheManager.getCache();

      return cacheManager;
   }

   @Override
   protected SimpleServerAuthenticationProvider createAuthenticationProvider() {
      SimpleServerAuthenticationProvider sap = new SimpleServerAuthenticationProvider();
      sap.addUser("user", "realm", "password".toCharArray(), null);
      return sap;
   }

   @Test
   public void testAuthentication() {
      ConfigurationBuilder clientBuilder = initServerAndClient();
      clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("user", "realm", "password".toCharArray()));
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      RemoteCache<String, String> defaultRemote = remoteCacheManager.getCache();
      defaultRemote.put("a", "a");
      assertEquals("a", defaultRemote.get("a"));
   }

   @Test(expectedExceptions=TransportException.class)
   public void testAuthenticationFailWrongAuth() {
      ConfigurationBuilder clientBuilder = initServerAndClient();
      clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("user", "realm", "foobar".toCharArray()));
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCacheManager.getCache();
   }

   @Test(expectedExceptions=HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN006017:.*")
   public void testAuthenticationFailNoAuth() {
      ConfigurationBuilder clientBuilder = initServerAndClient(Collections.singletonMap(Sasl.POLICY_NOANONYMOUS, "true"));
      clientBuilder.security().authentication().disable();
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      cache.put("a", "a");
   }

   @Test
   public void testAuthenticationUsername() {
      ConfigurationBuilder clientBuilder = initServerAndClient();
      clientBuilder.security().authentication().username("user").realm("realm").password("password");
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      RemoteCache<String, String> defaultRemote = remoteCacheManager.getCache();
      defaultRemote.put("a", "a");
      assertEquals("a", defaultRemote.get("a"));
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = ".*ISPN004067.*")
   public void testAuthenticationUsernameWithCallbackFail() {
      ConfigurationBuilder clientBuilder = initServerAndClient();
      clientBuilder.security().authentication().username("user").realm("realm").password("password").callbackHandler(new TestCallbackHandler("user", "realm", "foobar".toCharArray()));
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
   }

}
