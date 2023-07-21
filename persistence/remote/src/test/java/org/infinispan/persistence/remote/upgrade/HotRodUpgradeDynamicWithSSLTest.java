package org.infinispan.persistence.remote.upgrade;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.commons.test.security.TestCertificates;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Same as {@link HotRodUpgradeWithSSLTest} but using remote store created dynamically.
 */
@Test(testName = "upgrade.hotrod.HotRodUpgradeDynamicWithSSLTest", groups = "functional")
public class HotRodUpgradeDynamicWithSSLTest extends HotRodUpgradeWithSSLTest {

   @Override
   protected TestCluster configureTargetCluster() {
      return new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .withSSLKeyStore(TestCertificates.certificate("client"), TestCertificates.KEY_PASSWORD)
            .withSSLTrustStore(TestCertificates.certificate("ca"), TestCertificates.KEY_PASSWORD)
            .withHotRodBuilder(getHotRodServerBuilder())
            .cache().name(OLD_CACHE)
            .cache().name(TEST_CACHE)
            .build();
   }

   @Override
   protected void connectTargetCluster() {
      ConfigurationBuilder remoteStoreOldCache = createStoreBuilder(OLD_CACHE, OLD_PROTOCOL_VERSION);
      ConfigurationBuilder remoteStoreNewCache = createStoreBuilder(TEST_CACHE, NEW_PROTOCOL_VERSION);

      targetCluster.connectSource(OLD_CACHE, remoteStoreOldCache.build().persistence().stores().get(0));
      targetCluster.connectSource(TEST_CACHE, remoteStoreNewCache.build().persistence().stores().get(0));
   }

   private ConfigurationBuilder createStoreBuilder(String cacheName, ProtocolVersion version) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      RemoteStoreConfigurationBuilder storeBuilder = builder.persistence().addStore(RemoteStoreConfigurationBuilder.class);
      storeBuilder.remoteCacheName(cacheName).rawValues(true).protocolVersion(version).shared(true).segmented(false)
            .remoteSecurity().ssl().enable().trustStoreFileName(TestCertificates.certificate("ca")).trustStorePassword(TestCertificates.KEY_PASSWORD)
            .keyStoreFileName(TestCertificates.certificate("client")).keyStorePassword(TestCertificates.KEY_PASSWORD).sniHostName("server")
            .addServer().host("localhost").port(sourceCluster.getHotRodPort());

      return builder;
   }
}
