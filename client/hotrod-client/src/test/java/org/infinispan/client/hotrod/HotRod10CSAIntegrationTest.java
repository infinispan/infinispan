package org.infinispan.client.hotrod;

import org.testng.annotations.Test;

import java.util.Properties;

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
   protected void setHotRodProtocolVersion(Properties props) {
      props.setProperty("infinispan.client.hotrod.protocol_version", "1.0");
   }

}
