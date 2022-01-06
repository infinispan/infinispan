package org.infinispan.persistence.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.io.IOException;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
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
   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;

   @Override
   protected Configuration buildConfig(ConfigurationBuilder builder) {
      localCacheManager = TestCacheManagerFactory.createCacheManager(
            new GlobalConfigurationBuilder().defaultCacheName(REMOTE_CACHE),
            hotRodCacheConfiguration(builder));

      ClassLoader cl = RemoteStoreSSLTest.class.getClassLoader();
      // Unfortunately BaseNonBlockingStore stops and restarts the store, which can start a second hrServer - prevent that
      if (hrServer == null) {
         localCacheManager.getCache(REMOTE_CACHE);
         TestingUtil.replaceComponent(localCacheManager, TimeService.class, timeService, true);
         localCacheManager.getCache(REMOTE_CACHE).getAdvancedCache().getComponentRegistry().rewire();

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
         hrServer.start(serverBuilder.build(), localCacheManager);
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
   protected NonBlockingStore createStore() throws Exception {
      return new RemoteStore();
   }

   @Override
   protected PersistenceMarshaller getMarshaller() {
      return TestingUtil.extractPersistenceMarshaller(localCacheManager);
   }

   @Override
   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      configuration = null;
      super.tearDown();
      HotRodClientTestingUtil.killServers(hrServer);
      hrServer = null;
      TestingUtil.killCacheManagers(localCacheManager);
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
   protected boolean storePurgesAllExpired() {
      return false;
   }
}
