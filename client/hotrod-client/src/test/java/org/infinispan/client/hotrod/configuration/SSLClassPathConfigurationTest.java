package org.infinispan.client.hotrod.configuration;


import static org.testng.Assert.assertNotNull;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.util.SslContextFactory;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.configuration.SSLClassPathConfigurationTest", groups = "functional")
public class SSLClassPathConfigurationTest {

   public void testLoadTrustStore() {
      String keyStoreFileName = getClass().getResource("/keystore.jks").getPath();
      String truststoreFileName = "classpath:truststore2.jks";
      char[] password = "secret".toCharArray();

      SSLContext context =
              SslContextFactory.getContext(keyStoreFileName, password, truststoreFileName, password);

      assertNotNull(context);
   }

}
