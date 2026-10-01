package org.infinispan.persistence.remote.upgrade;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.testing.security.TestCertificates;
import org.testng.annotations.Test;

@Test(testName = "upgrade.hotrod.HotRodUpgradeWithSSLTest", groups = "functional")
public class HotRodUpgradeWithSSLTest extends HotRodUpgradeSynchronizerTest {


    @Override
   protected TestCluster createSourceCluster() throws Exception {
      HotRodServerConfigurationBuilder sourceHotRodBuilder = new HotRodServerConfigurationBuilder();
      sourceHotRodBuilder
            .ssl()
            .enable()
            .requireClientAuth(true)
            .keyStoreFileName(TestCertificates.certificate("server"))
            .keyStorePassword(TestCertificates.KEY_PASSWORD)
            .keyAlias("server")
            .trustStoreFileName(TestCertificates.certificate("ca"))
            .trustStorePassword(TestCertificates.KEY_PASSWORD);
      return new TestCluster.Builder().setName("sourceCluster").setNumMembers(2)
            .withSSLKeyStore(TestCertificates.certificate("client"), TestCertificates.KEY_PASSWORD)
            .withSSLTrustStore(TestCertificates.certificate("ca"), TestCertificates.KEY_PASSWORD)
            .withHotRodBuilder(sourceHotRodBuilder)
            .cache().name(OLD_CACHE)
            .cache().name(TEST_CACHE)
            .build();
   }

    @Override
    protected void reconnectMigration(TestCluster target) {
       // Re-establish the SSL migration remote stores that a previous test's disconnectSource cleared.
       target.connectSource(OLD_CACHE, buildSslRemoteStoreConfig(OLD_CACHE, OLD_PROTOCOL_VERSION));
       target.connectSource(TEST_CACHE, buildSslRemoteStoreConfig(TEST_CACHE, NEW_PROTOCOL_VERSION));
    }

    private StoreConfiguration buildSslRemoteStoreConfig(String cacheName, ProtocolVersion version) {
       ConfigurationBuilder builder = new ConfigurationBuilder();
       RemoteStoreConfigurationBuilder store = builder.persistence().addStore(RemoteStoreConfigurationBuilder.class);
       store.remoteCacheName(cacheName).protocolVersion(version).shared(true).segmented(false)
             .remoteSecurity().ssl().enable()
                .trustStoreFileName(TestCertificates.certificate("ca")).trustStorePassword(TestCertificates.KEY_PASSWORD)
                .keyStoreFileName(TestCertificates.certificate("client")).keyStorePassword(TestCertificates.KEY_PASSWORD)
                .sniHostName("server")
             .addServer().host("localhost").port(sourceCluster.getHotRodPort());
       return store.build().persistence().stores().get(0);
    }

    @Override
    protected TestCluster configureTargetCluster() {
       return new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .withSSLKeyStore(TestCertificates.certificate("client"), TestCertificates.KEY_PASSWORD)
            .withSSLTrustStore(TestCertificates.certificate("ca"), TestCertificates.KEY_PASSWORD)
            .withHotRodBuilder(getHotRodServerBuilder())
            .cache().name(OLD_CACHE).remotePort(sourceCluster.getHotRodPort()).remoteProtocolVersion(OLD_PROTOCOL_VERSION).remoteStoreProperty(RemoteStore.MIGRATION, "true")
            .cache().name(TEST_CACHE).remotePort(sourceCluster.getHotRodPort()).remoteProtocolVersion(NEW_PROTOCOL_VERSION).remoteStoreProperty(RemoteStore.MIGRATION, "true")
            .build();
   }

   HotRodServerConfigurationBuilder getHotRodServerBuilder() {
      HotRodServerConfigurationBuilder targetHotRodBuilder = new HotRodServerConfigurationBuilder();
      targetHotRodBuilder
            .ssl()
            .enable()
            .requireClientAuth(true)
            .keyStoreFileName(TestCertificates.certificate("server"))
            .keyStorePassword(TestCertificates.KEY_PASSWORD)
            .keyAlias("server")
            .trustStoreFileName(TestCertificates.certificate("ca"))
            .trustStorePassword(TestCertificates.KEY_PASSWORD);
      return targetHotRodBuilder;
   }
}
