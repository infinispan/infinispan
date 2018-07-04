package org.infinispan.persistence.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.persistence.remote.configuration.SecurityConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.server.core.security.simple.SimpleServerAuthenticationProvider;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.TimeService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * @author Tristan Tarrant
 * @since 9.1
 */
@Test(testName = "persistence.remote.RemoteStoreSSLTest", groups = "functional")
public class RemoteStoreSSLTest extends BaseStoreTest {

   private static final String REMOTE_CACHE = "remote-cache";
   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      ConfigurationBuilder localBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);

      localCacheManager = TestCacheManagerFactory.createCacheManager(
            new GlobalConfigurationBuilder().defaultCacheName(REMOTE_CACHE),
            hotRodCacheConfiguration(localBuilder));

      localCacheManager.getCache(REMOTE_CACHE);
      GlobalComponentRegistry gcr = localCacheManager.getGlobalComponentRegistry();
      gcr.registerComponent(timeService, TimeService.class);
      gcr.rewire();
      localCacheManager.getCache(REMOTE_CACHE).getAdvancedCache().getComponentRegistry().rewire();
      ClassLoader cl = RemoteStoreSSLTest.class.getClassLoader();
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

      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      SecurityConfigurationBuilder remoteSecurity = builder
            .persistence()
            .addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName(REMOTE_CACHE)
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
      RemoteStore remoteStore = new RemoteStore();
      remoteStore.init(createContext(builder.build()));

      return remoteStore;
   }

   @Override
   protected Marshaller getMarshaller() {
      return localCacheManager.getCache(REMOTE_CACHE).getAdvancedCache().getComponentRegistry().getUserMarshaller();
   }

   @Override
   @AfterMethod
   public void tearDown() {
      HotRodClientTestingUtil.killServers(hrServer);
      TestingUtil.killCacheManagers(localCacheManager);
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }

   @Override
   public void testLoadAll() throws PersistenceException {
   }

   @Override
   public void testReplaceExpiredEntry() throws Exception {
      cl.write(marshalledEntry(internalCacheEntry("k1", "v1", 100l)));
      timeService.advance(1101);
      assertNull(cl.load("k1"));
      long start = System.currentTimeMillis();
      cl.write(marshalledEntry(internalCacheEntry("k1", "v2", 100l)));
      assertTrue(cl.load("k1").getValue().equals("v2") || TestingUtil.moreThanDurationElapsed(start, 100));
   }
}
