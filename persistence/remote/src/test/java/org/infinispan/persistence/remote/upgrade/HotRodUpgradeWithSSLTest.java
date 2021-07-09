package org.infinispan.persistence.remote.upgrade;

import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "upgrade.hotrod.HotRodUpgradeWithSSLTest", groups = "functional")
public class HotRodUpgradeWithSSLTest extends HotRodUpgradeSynchronizerTest {

   protected static final char[] PASSWORD = "secret".toCharArray();
   protected final ClassLoader cl = HotRodUpgradeWithSSLTest.class.getClassLoader();
   protected final String trustStorePath = cl.getResource("ca.jks").getPath();
   protected final String keyStoreClientPath = cl.getResource("keystore_client.jks").getPath();
   protected final String keyStoreServerPath = cl.getResource("keystore_server.jks").getPath();

   @BeforeMethod
   public void setup() throws Exception {
      HotRodServerConfigurationBuilder sourceHotRodBuilder = new HotRodServerConfigurationBuilder();
      sourceHotRodBuilder
            .ssl()
            .enable()
            .requireClientAuth(true)
            .keyStoreFileName(keyStoreServerPath)
            .keyStorePassword(PASSWORD)
            .keyAlias("hotrod")
            .trustStoreFileName(trustStorePath)
            .trustStorePassword(PASSWORD);
      sourceCluster = new TestCluster.Builder().setName("sourceCluster").setNumMembers(2)
            .withSSLKeyStore(keyStoreClientPath, PASSWORD)
            .withSSLTrustStore(trustStorePath, PASSWORD)
            .withHotRodBuilder(sourceHotRodBuilder)
            .cache().name(OLD_CACHE)
            .cache().name(TEST_CACHE)
            .build();

      targetCluster = configureTargetCluster();
   }

   @Override
   protected TestCluster configureTargetCluster() {
      return new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .withSSLKeyStore(keyStoreClientPath, PASSWORD)
            .withSSLTrustStore(trustStorePath, PASSWORD)
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
            .keyStoreFileName(keyStoreServerPath)
            .keyStorePassword(PASSWORD)
            .keyAlias("hotrod")
            .trustStoreFileName(trustStorePath)
            .trustStorePassword(PASSWORD);
      return targetHotRodBuilder;
   }
}
