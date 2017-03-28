package org.infinispan.server.hotrod;

import java.lang.reflect.Method;

import org.testng.annotations.Test;

/**
 * Tests that Hot Rod server can shutdown even if client dies not close connection.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodShutdownTest")
public class HotRodShutdownTest extends HotRodSingleNodeTest {

   @Override
   protected void shutdownClient() {
   }

   public void testPutBasic(Method m) {
      client().assertPut(m);
   }

}
