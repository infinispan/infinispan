package org.infinispan.client.hotrod.configuration;


import static org.testng.Assert.assertNotNull;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.util.SslContextFactory;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.configuration.SSLClassPathConfigurationTest", groups = "functional")
public class SSLClassPathConfigurationTest {

   public void testLoadTrustStore() {
      String keyStoreFileName = getClass().getResource("/keystore_client.p12").getPath();
      String truststoreFileName = "classpath:ca.p12";
      char[] password = "secret".toCharArray();

      SSLContext context =
              new SslContextFactory()
                    .keyStoreFileName(keyStoreFileName)
                    .keyStoreType("pkcs12")
                    .keyStorePassword(password)
                    .trustStoreFileName(truststoreFileName)
                    .trustStoreType("pkcs12")
                    .trustStorePassword(password).getContext();

      assertNotNull(context);
   }

}
