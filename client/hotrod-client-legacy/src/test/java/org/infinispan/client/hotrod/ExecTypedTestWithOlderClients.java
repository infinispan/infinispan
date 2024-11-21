package org.infinispan.client.hotrod;

import org.testng.annotations.Test;

/**
 * @since 9.4
 */
@Test(groups = "functional", testName = "client.hotrod.ExecTypedTestWithOlderClients")
public class ExecTypedTestWithOlderClients extends ExecTypedTest {

   @Override
   protected ProtocolVersion getProtocolVersion() {
      return ProtocolVersion.PROTOCOL_VERSION_25;
   }
}
