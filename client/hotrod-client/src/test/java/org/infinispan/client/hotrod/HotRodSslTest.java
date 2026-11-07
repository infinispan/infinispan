package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.Consumer;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.security.TestCertificates;
import org.testng.annotations.Test;

/**
 * Tests SSL connections with various configurations (e.g. hostname validation disabled).
 *
 * @since 16.4
 */
@Test(testName = "client.hotrod.HotRodSslTest", groups = "functional")
@CleanupAfterMethod
public class HotRodSslTest extends SingleCacheManagerTest {

   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      return cacheManager;
   }

   @Override
   protected void teardown() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
      hotrodServer = null;
      super.teardown();
   }

   private void setupServer(Consumer<HotRodServerConfigurationBuilder> configurator) {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.ssl().enable()
            .keyStoreFileName(TestCertificates.certificate("server"))
            .keyStorePassword(TestCertificates.KEY_PASSWORD)
            .keyStoreType(TestCertificates.KEYSTORE_TYPE)
            .trustStoreFileName(TestCertificates.certificate("trust"))
            .trustStorePassword(TestCertificates.KEY_PASSWORD)
            .trustStoreType(TestCertificates.KEYSTORE_TYPE)
            .protocol("TLSv1.3");
      configurator.accept(serverBuilder);
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   private void setupClient(Consumer<ConfigurationBuilder> configurator) {
      ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder(hotrodServer);
      clientBuilder.security().ssl().enable()
            .keyStoreFileName(TestCertificates.certificate("client"))
            .keyStorePassword(TestCertificates.KEY_PASSWORD)
            .keyStoreType(TestCertificates.KEYSTORE_TYPE)
            .trustStoreFileName(TestCertificates.certificate("trust"))
            .trustStorePassword(TestCertificates.KEY_PASSWORD)
            .trustStoreType(TestCertificates.KEYSTORE_TYPE)
            .protocol("TLSv1.3");
      configurator.accept(clientBuilder);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
   }

   public void testHostnameValidationDisabled() {
      setupServer(c -> {});
      setupClient(c -> c.security().ssl().hostnameValidation(false));
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      cache.put(k(), v());
      assertEquals(v(), cache.get(k()));
   }

   public void testHostnameValidationEnabledWithSniHostName() {
      setupServer(c -> {});
      setupClient(c -> c.security().ssl().hostnameValidation(true).sniHostName("localhost"));
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      cache.put(k(), v());
      assertEquals(v(), cache.get(k()));
   }
}
