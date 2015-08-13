package org.infinispan.client.hotrod;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.RemoteAsyncAPIForceReturnPerCallTest")
public class RemoteAsyncAPIForceReturnPerCallTest extends RemoteAsyncAPITest {

   @Override
   protected boolean isForceReturnValuesViaConfiguration() {
      return false;
   }

   @Override
   protected RemoteCache<String, String> remote() {
      return super.remote().withFlags(Flag.FORCE_RETURN_VALUE);
   }

}
