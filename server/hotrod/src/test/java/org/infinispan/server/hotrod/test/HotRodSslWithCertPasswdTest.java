package org.infinispan.server.hotrod.test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.host;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.server.core.test.Stoppable;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Tests HotRod server start with SSL enabled and keystore which has different keystore and certificate passwords.
 *
 * @author vjuranek
 * @since 9.0
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodSslWithCertPasswdTest")
public class HotRodSslWithCertPasswdTest extends AbstractInfinispanTest {

   private String keyStoreFileName = getClass().getClassLoader().getResource("password_server_keystore.p12").getPath();
   private String trustStoreFileName =
         getClass().getClassLoader().getResource("password_client_truststore.p12").getPath();

   public void testServerStartWithSslAndCertPasswd() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.host(host()).port(serverPort()).idleTimeout(0);
      builder.ssl().enable().keyStoreFileName(keyStoreFileName).keyStorePassword("secret".toCharArray())
            .keyStoreType("pkcs12")
             .keyStoreCertificatePassword("secret2".toCharArray()).trustStoreFileName(trustStoreFileName)
             .trustStorePassword("secret".toCharArray()).trustStoreType("pkcs12");
      Stoppable.useCacheManager(createCacheManager(hotRodCacheConfiguration()), cm ->
            Stoppable.useServer(new HotRodServer(), server -> {
                                   server.start(builder.build(), cm);
                                   assertNotNull(server.getConfiguration().ssl().keyStoreCertificatePassword());
                                }
            ));
   }

}
