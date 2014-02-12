package org.infinispan.server.hotrod.stress

import java.lang.reflect.Method
import org.infinispan.test.TestingUtil.generateRandomString
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.server.hotrod.HotRodSingleNodeTest
import org.infinispan.server.hotrod.OperationStatus._
import org.testng.annotations.Test

/**
 * A simple test that stresses Hot Rod by storing big data and waits to allow
 * the test runner to generate heap dumps for the test.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = Array("profiling"), testName = "server.hotrod.stress.HotRodBigDataStressTest")
class HotRodBigDataStressTest extends HotRodSingleNodeTest {

   def testPutBigSizeValue(m: Method) {
      val value = generateRandomString(10 * 1024 * 1024).getBytes
      assertStatus(client.put(k(m), 0, 0, value), Success)
      while (true)
         Thread.sleep(5000)
   }

}
