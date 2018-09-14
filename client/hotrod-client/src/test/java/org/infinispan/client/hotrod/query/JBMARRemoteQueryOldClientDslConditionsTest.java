package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.query.JBMARRemoteQueryOldClientDslConditionsTest")
public class JBMARRemoteQueryOldClientDslConditionsTest extends JBMARRemoteQueryDslConditionsTest {

   @Override
   protected ProtocolVersion getProtocolVersion() {
      return ProtocolVersion.PROTOCOL_VERSION_27;
   }
}
