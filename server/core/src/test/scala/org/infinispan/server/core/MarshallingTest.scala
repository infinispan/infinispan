package org.infinispan.server.core

import org.testng.annotations.Test
import org.testng.Assert._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.core.MarshallingTest")
class MarshallingTest extends AbstractMarshallingTest {

   def testMarshallingBigByteArrayValue {
      val cacheValue = new CacheValue(getBigByteArray, 9)
      val bytes = marshaller.objectToByteBuffer(cacheValue)
      val readValue = marshaller.objectFromByteBuffer(bytes).asInstanceOf[CacheValue]
      assertEquals(readValue, cacheValue)
   }

}