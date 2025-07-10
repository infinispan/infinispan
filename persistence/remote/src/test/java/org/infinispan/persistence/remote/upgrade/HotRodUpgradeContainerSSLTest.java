package org.infinispan.persistence.remote.upgrade;

import java.util.Properties;

import org.infinispan.commons.test.security.TestCertificates;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.global.RemoteContainersConfigurationBuilder;
import org.testng.annotations.Test;

@Test(testName = "upgrade.hotrod.HotRodUpgradeContainerSSLTest", groups = "functional")
public class HotRodUpgradeContainerSSLTest extends HotRodUpgradeWithSSLTest {

   private static final String CONTAINER_NAME_OLD = "old-ssl-container-name";
   private static final String CONTAINER_NAME_TEST = "test-ssl-container-name";

   @Override
   protected TestCluster configureTargetCluster() {
      Properties properties = new Properties();
      properties.setProperty("infinispan.client.hotrod.async_executor_factory", "org.infinispan.executors.DefaultExecutorFactory");
      properties.setProperty("infinispan.client.hotrod.use_ssl", "true");
      properties.setProperty("infinispan.client.hotrod.key_store_file_name", TestCertificates.certificate("client"));
      properties.setProperty("infinispan.client.hotrod.key_store_password", new String(TestCertificates.KEY_PASSWORD));
      properties.setProperty("infinispan.client.hotrod.key_store_type", "JKS");
      properties.setProperty("infinispan.client.hotrod.trust_store_file_name", TestCertificates.certificate("ca"));
      properties.setProperty("infinispan.client.hotrod.trust_store_password", new String(TestCertificates.KEY_PASSWORD));
      properties.setProperty("infinispan.client.hotrod.trust_store_type", "JKS");
      properties.setProperty("infinispan.client.hotrod.ssl_hostname_validation", "false");

      return new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .withHotRodBuilder(getHotRodServerBuilder())
            .cache()
            .name(TEST_CACHE)
            .remotePort(sourceCluster.getHotRodPort())
            .useRemoteContainer(CONTAINER_NAME_TEST)
            .remoteProtocolVersion(NEW_PROTOCOL_VERSION)
            .remoteStoreProperty(RemoteStore.MIGRATION, "true")
            .cache()
            .name(OLD_CACHE)
            .remotePort(sourceCluster.getHotRodPort())
            .useRemoteContainer(CONTAINER_NAME_OLD)
            .remoteProtocolVersion(OLD_PROTOCOL_VERSION)
            .remoteStoreProperty(RemoteStore.MIGRATION, "true")
            .build(() -> {
               GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
               RemoteContainersConfigurationBuilder rccb = global.addModule(RemoteContainersConfigurationBuilder.class);

               Properties old = new Properties();
               old.putAll(properties);
               old.setProperty("infinispan.client.hotrod.protocol_version", OLD_PROTOCOL_VERSION.toString());
               rccb.addRemoteContainer(CONTAINER_NAME_OLD)
                     .uri(String.format("hotrods://localhost:%d", sourceCluster.getHotRodPort()))
                     .properties(old);

               Properties test = new Properties();
               test.putAll(properties);
               test.setProperty("infinispan.client.hotrod.protocol_version", NEW_PROTOCOL_VERSION.toString());
               rccb.addRemoteContainer(CONTAINER_NAME_TEST)
                     .uri(String.format("hotrods://localhost:%d", sourceCluster.getHotRodPort()))
                     .properties(test);

               return global;
            }, properties);
   }
}
