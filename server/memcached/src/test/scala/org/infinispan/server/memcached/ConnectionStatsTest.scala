package org.infinispan.server.memcached

import javax.management.{MBeanServer, ObjectName}

import org.infinispan.jmx.PerThreadMBeanServerLookup
import org.testng.Assert._

/**
 * Connection statistic tests that are common to all
 * Netty-based server implementations.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
object ConnectionStatsTest {

   def testSingleLocalConnection(jmxDomain: String, serverName: String) {
      val mbeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer
      val on = new ObjectName("%s:type=Server,name=%s,component=Transport"
              .format(jmxDomain, serverName))
      // Now verify that via JMX as well, these stats are also as expected
      assertTrue(mbeanServer.getAttribute(on,
         "TotalBytesRead").toString.toInt > 0)
      assertTrue(mbeanServer.getAttribute(on,
         "TotalBytesWritten").toString.toInt > 0)
      assertEquals(mbeanServer.getAttribute(on,
         "NumberOfLocalConnections").asInstanceOf[java.lang.Integer], 1)
   }

   def testMultipleLocalConnections(
           jmxDomain: String, serverName: String, expectedTotalConns: Int) {
      val mbeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer
      val on = new ObjectName("%s:type=Server,name=%s,component=Transport"
              .format(jmxDomain, serverName))

      assertEquals(mbeanServer.getAttribute(on,
            "NumberOfLocalConnections").asInstanceOf[java.lang.Integer],
            expectedTotalConns)
   }

   def testGlobalConnections(jmxDomain: String, serverName: String,
           expectedTotalConns: Int, mbeanServer: MBeanServer) {
      val on = new ObjectName("%s:type=Server,name=%s,component=Transport"
              .format(jmxDomain, serverName))
      // Now verify that via JMX as well, these stats are also as expected
      assertEquals(mbeanServer.getAttribute(on,
         "NumberOfGlobalConnections").asInstanceOf[java.lang.Integer],
         expectedTotalConns)
   }

}
