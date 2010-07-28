package org.infinispan.server.hotrod

import org.testng.annotations.Test
import org.testng.Assert._
import org.infinispan.commands.remote.ClusteredGetCommand
import org.infinispan.server.core.AbstractMarshallingTest
import org.infinispan.util.ByteArrayKey

/**
 * Tests marshalling of Hot Rod classes.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodMarshallingTest")
class HotRodMarshallingTest extends AbstractMarshallingTest {

   def testMarshallingBigByteArrayKey {
      val cacheKey = new ByteArrayKey(getBigByteArray)      
      val bytes = marshaller.objectToByteBuffer(cacheKey)
      val readKey = marshaller.objectFromByteBuffer(bytes).asInstanceOf[ByteArrayKey]
      assertEquals(readKey, cacheKey)
   }

   def testMarshallingCommandWithBigByteArrayKey {
      val cacheKey = new ByteArrayKey(getBigByteArray)
      val command = new ClusteredGetCommand(cacheKey, "mycache")
      val bytes = marshaller.objectToByteBuffer(command)
      val readCommand = marshaller.objectFromByteBuffer(bytes).asInstanceOf[ClusteredGetCommand]
      assertEquals(readCommand, command)
   }

}
