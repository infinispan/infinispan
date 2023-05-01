package org.infinispan.server.resp;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.OK;

@Test(groups = "functional", testName = "server.resp.ClusteredOperationsTest")
public class ClusteredOperationsTest extends BaseMultipleRespTest {

   public void testClusteredGetAndSet() {
      for (int i = 0; i < 100; i++) {
         assertThat(redisConnection1.sync().set("key" + i, "value" + i)).isEqualTo(OK);
      }


      for (int i = 0; i < 100; i++) {
         assertThat( redisConnection2.sync().get("key" + i)).isEqualTo("value" + i);
      }
   }
}
