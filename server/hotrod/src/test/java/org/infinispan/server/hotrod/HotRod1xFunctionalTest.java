package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.OperationStatus.OperationNotExecuted;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Optional;

import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestResponseWithPrevious;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.hotrod.HotRod1xFunctionalTest")
public class HotRod1xFunctionalTest extends HotRodFunctionalTest {
   @Override
   protected HotRodClient connectClient() {
      return new HotRodClient("127.0.0.1", hotRodServer.getPort(), cacheName, 60, (byte) 13);
   }

   @Override
   public void testSize(Method m) {
      // Not supported
   }

   @Override
   protected boolean assertSuccessPrevious(TestResponseWithPrevious resp, byte[] expected) {
      if (expected == null)
         assertEquals(Optional.empty(), resp.previous);
      else
         assertTrue(java.util.Arrays.equals(expected, resp.previous.get()));
      return assertStatus(resp, Success);
   }

   @Override
   protected boolean assertNotExecutedPrevious(TestResponseWithPrevious resp, byte[] expected) {
      if (expected == null)
         assertEquals(Optional.empty(), resp.previous);
      else
         assertTrue(java.util.Arrays.equals(expected, resp.previous.get()));
      return assertStatus(resp, OperationNotExecuted);
   }
}
