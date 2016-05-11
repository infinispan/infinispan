package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
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
   public void testAuthenticationFail() {
      ConfigurationBuilder clientBuilder = initServerAndClient();
      clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("user", "realm", "foobar".toCharArray()));
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCacheManager.getCache();
   }

}
