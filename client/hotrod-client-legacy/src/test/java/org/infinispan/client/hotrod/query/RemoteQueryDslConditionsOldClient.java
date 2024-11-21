package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.testng.annotations.Test;


@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryDslConditionsOldClient")
public class RemoteQueryDslConditionsOldClient extends RemoteQueryDslConditionsTest {

   @Override
   protected ProtocolVersion getProtocolVersion() {
      return ProtocolVersion.PROTOCOL_VERSION_27;
   }
}
