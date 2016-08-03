package org.infinispan.client.hotrod.configuration;


import org.infinispan.commons.util.SslContextFactory;
import static org.testng.Assert.assertNotNull;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;

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
