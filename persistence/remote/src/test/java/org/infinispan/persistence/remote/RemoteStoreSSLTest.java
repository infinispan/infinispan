package org.infinispan.persistence.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.persistence.remote.configuration.SecurityConfigurationBuilder;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.server.core.security.simple.SimpleServerAuthenticationProvider;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * @author Tristan Tarrant
 * @since 9.1
 */
@Test(testName = "persistence.remote.RemoteStoreSSLTest", groups = "functional")
public class RemoteStoreSSLTest extends BaseNonBlockingStoreTest {

   private static final String REMOTE_CACHE = "remote-cache";
   private EmbeddedCacheManager serverCacheManager;
   private Cache<Object, Object> serverCache;
   private HotRodServer hrServer;

   @Override
   protected Configuration buildConfig(ConfigurationBuilder builder) {
      serverCacheManager = TestCacheManagerFactory.createCacheManager(
            new GlobalConfigurationBuilder().defaultCacheName(REMOTE_CACHE),
            hotRodCacheConfiguration(builder));

      ClassLoader cl = RemoteStoreSSLTest.class.getClassLoader();
      // Unfortunately BaseNonBlockingStoreTest stops and restarts the store, which can start a second hrServer - prevent that
      if (hrServer == null) {
         serverCache = serverCacheManager.getCache(REMOTE_CACHE);
         TestingUtil.replaceComponent(serverCacheManager, TimeService.class, timeService, true);

         SimpleServerAuthenticationProvider sap = new SimpleServerAuthenticationProvider();
         HotRodServerConfigurationBuilder serverBuilder = HotRodTestingUtil.getDefaultHotRodConfiguration();
         serverBuilder
               .ssl()
               .enable()
               .requireClientAuth(true)
               .keyStoreFileName(cl.getResource("keystore_server.jks").getPath())
               .keyStorePassword("secret".toCharArray())
               .keyAlias("hotrod")
               .trustStoreFileName(cl.getResource("ca.jks").getPath())
               .trustStorePassword("secret".toCharArray());
         serverBuilder
               .authentication()
               .enable()
               .serverName("localhost")
               .addAllowedMech("EXTERNAL")
               .serverAuthenticationProvider(sap);
         hrServer = new HotRodServer();
         hrServer.start(serverBuilder.build(), serverCacheManager);
      }

      SecurityConfigurationBuilder remoteSecurity = builder
            .persistence()
            .addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName(REMOTE_CACHE)
            .shared(true)
            // Store cannot be segmented as the remote cache is LOCAL and it doesn't report its segments?
            .segmented(false)
            .remoteSecurity();
      remoteSecurity
            .ssl().enable()
            .keyStoreFileName(cl.getResource("keystore_client.jks").getPath())
            .keyStorePassword("secret".toCharArray())
            .trustStoreFileName(cl.getResource("ca.jks").getPath())
            .trustStorePassword("secret".toCharArray())
            .addServer()
            .host(hrServer.getHost())
            .port(hrServer.getPort());
      remoteSecurity
            .authentication().enable()
            .saslMechanism("EXTERNAL");

      return builder.build();
   }

   @Override
   protected NonBlockingStore<Object, Object> createStore() throws Exception {
      return new RemoteStore<>();
   }

   @Override
   protected PersistenceMarshaller getMarshaller() {
      return TestingUtil.extractPersistenceMarshaller(serverCacheManager);
   }

   @Override
   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      configuration = null;
      super.tearDown();
      HotRodClientTestingUtil.killServers(hrServer);
      hrServer = null;
      TestingUtil.killCacheManagers(serverCacheManager);
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }

   @Override
   protected Object keyToStorage(Object key) {
      try {
         return new WrappedByteArray(marshaller.objectToByteBuffer(key));
      } catch (IOException | InterruptedException e) {
         throw new AssertionError(e);
      }
   }

   @Override
   public void testApproximateSize() {
      // The server only reports the approximate size when the cache's statistics are enabled
      TestingUtil.findInterceptor(serverCache, CacheMgmtInterceptor.class).setStatisticsEnabled(true);

      super.testApproximateSize();

      TestingUtil.findInterceptor(serverCache, CacheMgmtInterceptor.class).setStatisticsEnabled(false);

      assertEquals(-1L, store.approximateSizeWait(segments));
   }
}
