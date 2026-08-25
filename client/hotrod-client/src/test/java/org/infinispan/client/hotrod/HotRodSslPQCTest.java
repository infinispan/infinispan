package org.infinispan.client.hotrod;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.Consumer;

import javax.net.ssl.SSLProtocolException;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.jdkspecific.SSLParametersHelper;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.security.TestCertificates;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @since 16.3
 */
@Test(testName = "client.hotrod.HotRodSslPQCTest", groups = "functional")
@CleanupAfterMethod
public class HotRodSslPQCTest extends SingleCacheManagerTest {

   private static final String X25519MLKEM768 = "X25519MLKEM768";
   private static final String X25519 = "x25519";
   private static final String SECP256R1MLKEM768 = "SecP256r1MLKEM768";

   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;

   @BeforeClass(alwaysRun = true)
   @Override
   protected void createBeforeClass() throws Throwable {
      if (!SSLParametersHelper.isNamedGroupAvailable(X25519MLKEM768)) {
         throw new SkipException(X25519MLKEM768 + " is not available in this JDK");
      }
      super.createBeforeClass();
   }

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
      serverBuilder.ssl().enable().keyStoreFileName(TestCertificates.certificate("server")).keyStorePassword(TestCertificates.KEY_PASSWORD).keyStoreType(TestCertificates.KEYSTORE_TYPE).trustStoreFileName(TestCertificates.certificate("trust")).trustStorePassword(TestCertificates.KEY_PASSWORD).trustStoreType(TestCertificates.KEYSTORE_TYPE).protocol("TLSv1.3");
      configurator.accept(serverBuilder);
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   private void setupClient(Consumer<ConfigurationBuilder> configurator) {
      ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder(hotrodServer);
      clientBuilder.security().ssl().keyStoreFileName(TestCertificates.certificate("client")).keyStorePassword(TestCertificates.KEY_PASSWORD).keyStoreType(TestCertificates.KEYSTORE_TYPE).trustStoreFileName(TestCertificates.certificate("trust")).trustStorePassword(TestCertificates.KEY_PASSWORD).trustStoreType(TestCertificates.KEYSTORE_TYPE).sniHostName("localhost").protocol("TLSv1.3");
      configurator.accept(clientBuilder);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
   }

   public void testDefaults() {
      setupServer(c -> {});
      setupClient(c -> {});
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      cache.put(k(), v());
      assertEquals(v(), cache.get(k()));
   }

   public void testSameGroups() {
      setupServer(c -> c.ssl().namedGroups(X25519MLKEM768));
      setupClient(c -> c.security().ssl().namedGroups(X25519MLKEM768));
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      cache.put(k(), v());
      assertEquals(v(), cache.get(k()));
   }

   public void testFallbackGroups() {
      setupServer(c -> c.ssl().namedGroups(X25519MLKEM768, X25519));
      setupClient(c -> c.security().ssl().namedGroups(X25519));
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      cache.put(k(), v());
      assertEquals(v(), cache.get(k()));
   }

   public void testDifferentGroups() {
      setupServer(c -> c.ssl().namedGroups(SECP256R1MLKEM768));
      setupClient(c -> c.security().ssl().namedGroups(X25519MLKEM768));
      assertThatThrownBy(() -> remoteCacheManager.getCache()).hasCauseInstanceOf(SSLProtocolException.class);
   }
}
