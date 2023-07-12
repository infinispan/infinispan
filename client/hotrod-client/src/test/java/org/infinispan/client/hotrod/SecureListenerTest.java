package org.infinispan.client.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.event.EventLogListener;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.server.core.security.simple.SimpleSaslAuthenticator;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.TestCallbackHandler;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests verifying client listeners with authentication enabled.
 *
 * @author Dan Berindei
 */
@Test(testName = "client.hotrod.SecureListenerTest", groups = "functional")
public class SecureListenerTest extends AbstractAuthenticationTest {
   static final Subject ADMIN = TestingUtil.makeSubject("admin");
   static final String CACHE_NAME = "secured-listen";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles =
            global.security().authorization().enable().principalRoleMapper(new IdentityRoleMapper()).groupOnlyMapping(false);
      globalRoles.role("admin").permission(AuthorizationPermission.ALL)
                 .role("RWLuser").permission(AuthorizationPermission.READ, AuthorizationPermission.WRITE,
                                             AuthorizationPermission.LISTEN)
                 .role("RWUser").permission(AuthorizationPermission.READ, AuthorizationPermission.WRITE);
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.security().authorization().enable()
            .role("admin").role("RWLuser").role("RWUser");
      config.encoding().key().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      config.encoding().value().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      cacheManager = TestCacheManagerFactory.createCacheManager(global, config);
      cacheManager.defineConfiguration(CACHE_NAME, config.build());
      cacheManager.getCache();

      hotrodServer = initServer(Collections.emptyMap(), 0);

      return cacheManager;
   }

   @Override
   protected SimpleSaslAuthenticator createAuthenticationProvider() {
      SimpleSaslAuthenticator sap = new SimpleSaslAuthenticator();
      sap.addUser("RWLuser", "realm", "password".toCharArray(), null);
      sap.addUser("RWuser", "realm", "password".toCharArray(), null);
      return sap;
   }

   @Override
   protected void setup() throws Exception {
      Security.doAs(ADMIN, () -> {
         try {
            SecureListenerTest.super.setup();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      });
   }

   @Override
   protected void teardown() {
      Security.doAs(ADMIN, () -> SecureListenerTest.super.teardown());
   }

   @Override
   protected void clearCacheManager() {
      Security.doAs(ADMIN, () -> cacheManager.getCache().clear());

      if (remoteCacheManager != null) {
         HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
         remoteCacheManager = null;
      }
   }

   @Override
   protected HotRodServer initServer(Map<String, String> mechProperties, int index) {
      return Security.doAs(ADMIN, () -> SecureListenerTest.super.initServer(mechProperties, index));
   }

   public void testImplicitRemoveOnClose() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = newClientBuilder();
      clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("RWLuser",null, "password"));

      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      RemoteCache<Object, Object> clientCache = remoteCacheManager.getCache(CACHE_NAME);

      EventLogListener<Object> listener = new EventLogListener<>(clientCache);
      clientCache.addClientListener(listener);

      Cache<Object, Object> serverCache = cacheManager.getCache(CACHE_NAME);
      CacheNotifier cacheNotifier = TestingUtil.extractComponent(serverCache, CacheNotifier.class);
      assertEquals(1, cacheNotifier.getListeners().size());

      clientCache.put("key", "value");

      listener.expectSingleEvent("key", ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);

      remoteCacheManager.close();

      eventuallyEquals(0, () -> cacheNotifier.getListeners().size());
   }

   public void testAddListenerWithoutPermission() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = newClientBuilder();
      clientBuilder.security().authentication().saslMechanism("CRAM-MD5").username("RWuser").password("password");

      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      RemoteCache<Object, Object> clientCache = remoteCacheManager.getCache(CACHE_NAME);

      EventLogListener<Object> listener = new EventLogListener<>(clientCache);
      Exceptions.expectException(HotRodClientException.class, () -> clientCache.addClientListener(listener));

      Cache<Object, Object> serverCache = cacheManager.getCache(CACHE_NAME);
      CacheNotifier cacheNotifier = TestingUtil.extractComponent(serverCache, CacheNotifier.class);
      assertEquals(0, cacheNotifier.getListeners().size());

      remoteCacheManager.close();

      assertEquals(0, cacheNotifier.getListeners().size());
   }
}
