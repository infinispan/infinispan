package org.infinispan.server.hotrod

import org.testng.annotations.Test
import java.lang.reflect.Method
import io.netty.channel.ChannelFuture

/**
 * Tests that Hot Rod server can shutdown even if client dies not close connection.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodShutdownTest")
class HotRodShutdownTest extends HotRodSingleNodeTest {

   override protected def shutdownClient: ChannelFuture = null

   def testPutBasic(m: Method) {
      client.assertPut(m)
   }

}