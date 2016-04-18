package org.infinispan.server.hotrod

import org.testng.annotations.Test
import org.testng.Assert._
import org.infinispan.server.core.test.Stoppable
import org.infinispan.test.fwk.TestCacheManagerFactory._
import test.HotRodTestingUtil._
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.test.AbstractInfinispanTest

/**
 * Hot Rod server unit test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodServerTest")
class HotRodServerTest extends AbstractInfinispanTest {

   def testValidateProtocolServerNullProperties() {
      Stoppable.useCacheManager(createCacheManager(hotRodCacheConfiguration())) { cm =>
         Stoppable.useServer(new HotRodServer) { server =>
            server.start(new HotRodServerConfigurationBuilder().build, cm)
            assertEquals(server.getHost, "127.0.0.1")
            assertEquals(server.getPort, 11222)
         }
      }
   }

}
