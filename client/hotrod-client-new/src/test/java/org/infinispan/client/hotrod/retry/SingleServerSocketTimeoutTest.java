package org.infinispan.client.hotrod.retry;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.retry.SingleServerSocketTimeoutTest")
public class SingleServerSocketTimeoutTest extends SocketTimeoutFailureRetryTest {

   {
      // This reproduces the case when an operation times out but there is only a single server.
      // The operation is then registered again on the same channel and it succeeds.
      nbrOfServers = 1;
   }
}
