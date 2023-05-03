package org.infinispan.server.resp;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.resp.ClusteredOperationsTest")
public class ClusteredOperationsTest extends BaseMultipleRespTest {

   public void testClusteredGetAndSet() {
      for (int i = 0; i < 100; i++) {
         assertEquals("OK", redisConnection1.sync().set("key" + i, "value" + i));
      }


      for (int i = 0; i < 100; i++) {
         assertEquals("value" + i, redisConnection2.sync().get("key" + i));
      }
   }
}
