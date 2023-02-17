package org.infinispan.persistence.remote.upgrade;

import org.infinispan.commons.test.security.TestCertificates;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "upgrade.hotrod.HotRodUpgradeWithSSLTest", groups = "functional")
public class HotRodUpgradeWithSSLTest extends HotRodUpgradeSynchronizerTest {


   @BeforeMethod
   public void setup() throws Exception {
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
      sourceCluster = new TestCluster.Builder().setName("sourceCluster").setNumMembers(2)
            .withSSLKeyStore(TestCertificates.certificate("client"), TestCertificates.KEY_PASSWORD)
            .withSSLTrustStore(TestCertificates.certificate("ca"), TestCertificates.KEY_PASSWORD)
            .withHotRodBuilder(sourceHotRodBuilder)
            .cache().name(OLD_CACHE)
            .cache().name(TEST_CACHE)
            .build();

      targetCluster = configureTargetCluster();
   }

   @Override
   protected TestCluster configureTargetCluster() {
      return new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .withSSLKeyStore(TestCertificates.certificate("client"), TestCertificates.KEY_PASSWORD)
            .withSSLTrustStore(TestCertificates.certificate("ca"), TestCertificates.KEY_PASSWORD)
            .withHotRodBuilder(getHotRodServerBuilder())
            .cache().name(OLD_CACHE).remotePort(sourceCluster.getHotRodPort()).remoteProtocolVersion(OLD_PROTOCOL_VERSION)
            .cache().name(TEST_CACHE).remotePort(sourceCluster.getHotRodPort()).remoteProtocolVersion(NEW_PROTOCOL_VERSION)
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
