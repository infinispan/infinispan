package org.infinispan.client.hotrod.configuration;


import static org.testng.Assert.assertNotNull;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.test.security.TestCertificates;
import org.infinispan.commons.util.SslContextFactory;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.configuration.SSLClassPathConfigurationTest", groups = "functional")
public class SSLClassPathConfigurationTest {

   public void testLoadTrustStore() {
      String keyStoreFileName = TestCertificates.certificate("client");
      String truststoreFileName = "classpath:ca.pfx";

      SSLContext context =
            new SslContextFactory()
                  .keyStoreFileName(keyStoreFileName)
                  .keyStoreType(TestCertificates.KEYSTORE_TYPE)
                  .keyStorePassword(TestCertificates.KEY_PASSWORD)
                  .trustStoreFileName(truststoreFileName)
                  .trustStoreType(TestCertificates.KEYSTORE_TYPE)
                  .trustStorePassword(TestCertificates.KEY_PASSWORD).build().sslContext();

      assertNotNull(context);
   }
}
