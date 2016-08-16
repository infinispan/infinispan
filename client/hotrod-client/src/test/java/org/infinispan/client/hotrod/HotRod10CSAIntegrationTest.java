package org.infinispan.client.hotrod;

import org.testng.annotations.Test;

/**
 * Tests consistent hash algorithm consistency between the client and server
 * using Hot Rod's 1.0 protocol.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "client.hotrod.HotRod10CSAIntegrationTest")
public class HotRod10CSAIntegrationTest extends CSAIntegrationTest {

   @Override
   protected void setHotRodProtocolVersion(org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder) {
      clientBuilder.protocolVersion("1.0");
   }

}
