package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * HotRod client - server test with SSL enabled and keystore which has different keystore and certificate passwords.
 *
 * @author vjuranek
 * @since 9.0
 */
@Test(testName = "client.hotrod.SslWithCertPasswdTest", groups = "functional")
@CleanupAfterMethod
public class SslWithCertPasswdTest extends SslTest {

   @Override
   protected void initServerAndClient(boolean sslServer, boolean sslClient) {
      hotrodServer = new HotRodServer();
      HotRodServerConfigurationBuilder serverBuilder = HotRodTestingUtil.getDefaultHotRodConfiguration();

      ClassLoader tccl = Thread.currentThread().getContextClassLoader();
      String keyStoreFileName = tccl.getResource("keystore2.jks").getPath();
      String trustStoreFileName = tccl.getResource("truststore2.jks").getPath();
      serverBuilder.ssl()
         .enabled(sslServer)
         .keyStoreFileName(keyStoreFileName)
         .keyStorePassword("secret".toCharArray())
         .keyStoreCertificatePassword("changeme".toCharArray())
         .trustStoreFileName(trustStoreFileName)
         .trustStorePassword("secret".toCharArray());
      hotrodServer.start(serverBuilder.build(), cacheManager);

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder
         .addServer()
            .host("127.0.0.1")
            .port(hotrodServer.getPort())
            .socketTimeout(3000)
         .connectionPool()
            .maxActive(1)
         .security()
            .authentication()
            .disable()
            .ssl()
               .enabled(sslClient)
               .keyStoreFileName(keyStoreFileName)
               .keyStorePassword("secret".toCharArray())
               .keyStoreCertificatePassword("changeme".toCharArray())
               .trustStoreFileName(trustStoreFileName)
               .trustStorePassword("secret".toCharArray())
         .connectionPool()
            .timeBetweenEvictionRuns(2000);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      defaultRemote = remoteCacheManager.getCache();
   }

}
