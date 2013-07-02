package org.infinispan.server.core

import org.testng.annotations.Test
import org.testng.AssertJUnit._
import java.util

/**
 * Marshalling test for server core classes.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.core.MarshallingTest")
class MarshallingTest extends AbstractMarshallingTest {

   def testMarshallingBigByteArrayValue {
      val cacheValue = getBigByteArray
      val bytes = marshaller.objectToByteBuffer(cacheValue)
      val readValue = marshaller.objectFromByteBuffer(bytes).asInstanceOf[Array[Byte]]
      assertTrue(util.Arrays.equals(readValue, cacheValue))
   }

}