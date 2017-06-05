package org.infinispan.persistence.remote.upgrade;

import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "upgrade.hotrod.HotRodUpgradeWithSSLTest", groups = "functional")
public class HotRodUpgradeWithSSLTest extends HotRodUpgradeSynchronizerTest {
   @BeforeMethod
   public void setup() throws Exception {
      ClassLoader cl = HotRodUpgradeWithSSLTest.class.getClassLoader();
      HotRodServerConfigurationBuilder sourceHotRodBuilder = new HotRodServerConfigurationBuilder();
      sourceHotRodBuilder
            .ssl()
            .enable()
            .requireClientAuth(true)
            .keyStoreFileName(cl.getResource("keystore_server.jks").getPath())
            .keyStorePassword("secret".toCharArray())
            .keyAlias("hotrod")
            .trustStoreFileName(cl.getResource("ca.jks").getPath())
            .trustStorePassword("secret".toCharArray());
      sourceCluster = new TestCluster.Builder().setName("sourceCluster").setNumMembers(1)
            .withSSLKeyStore(cl.getResource("keystore_client.jks").getPath(), "secret".toCharArray())
            .withSSLTrustStore(cl.getResource("ca.jks").getPath(), "secret".toCharArray())
            .withHotRodBuilder(sourceHotRodBuilder)
            .cache().name(OLD_CACHE)
            .cache().name(TEST_CACHE)
            .build();

      HotRodServerConfigurationBuilder targetHotRodBuilder = new HotRodServerConfigurationBuilder();
      targetHotRodBuilder
            .ssl()
            .enable()
            .requireClientAuth(true)
            .keyStoreFileName(cl.getResource("keystore_server.jks").getPath())
            .keyStorePassword("secret".toCharArray())
            .keyAlias("hotrod")
            .trustStoreFileName(cl.getResource("ca.jks").getPath())
            .trustStorePassword("secret".toCharArray());
      targetCluster = new TestCluster.Builder().setName("targetCluster").setNumMembers(1)
            .withSSLKeyStore(cl.getResource("keystore_client.jks").getPath(), "secret".toCharArray())
            .withSSLTrustStore(cl.getResource("ca.jks").getPath(), "secret".toCharArray())
            .withHotRodBuilder(targetHotRodBuilder)
            .cache().name(OLD_CACHE).remotePort(sourceCluster.getHotRodPort()).remoteProtocolVersion(OLD_PROTOCOL_VERSION)
            .cache().name(TEST_CACHE).remotePort(sourceCluster.getHotRodPort()).remoteProtocolVersion(NEW_PROTOCOL_VERSION)
            .build();
   }
}
