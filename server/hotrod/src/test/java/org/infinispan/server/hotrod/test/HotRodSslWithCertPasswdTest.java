package org.infinispan.server.hotrod.test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.host;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.ssl.SslContextName;
import org.infinispan.server.core.test.Stoppable;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Factory;
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

   private final String sslProvider;

   public HotRodSslWithCertPasswdTest(String sslProvider) {
      this.sslProvider = sslProvider;
   }

   @Override
   protected String parameters() {
      return "[sslProvider=" + sslProvider + "]";
   }

   @Factory
   public Object[] defaultFactory() {
      List<Object> instances = new ArrayList<>();
      for (Object[] sslProviderParam : SslContextName.PROVIDER) {
         instances.add(new HotRodSslWithCertPasswdTest(sslProviderParam[0].toString()));
      }
      return instances.toArray();
   }

   public void testServerStartWithSslAndCertPasswd() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.host(host()).port(serverPort()).idleTimeout(0);
      builder.ssl().enable().keyStoreFileName(keyStoreFileName).keyStorePassword("secret".toCharArray())
             .keyStoreType("pkcs12")
             .keyStoreCertificatePassword("secret2".toCharArray()).trustStoreFileName(trustStoreFileName)
             .trustStorePassword("secret".toCharArray()).trustStoreType("pkcs12");
      Stoppable.useCacheManager(createCacheManager(hotRodCacheConfiguration()), cm ->
            Stoppable.useServer(HotRodTestingUtil.startHotRodServer(cm, builder), server -> {
                                   assertNotNull(server.getConfiguration().ssl().keyStoreCertificatePassword());
                                }
            ));
   }

}
