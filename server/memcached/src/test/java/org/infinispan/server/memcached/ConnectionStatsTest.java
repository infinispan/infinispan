package org.infinispan.server.memcached;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.infinispan.commons.jmx.PerThreadMBeanServerLookup;

/**
 * Connection statistic tests that are common to all
 * Netty-based server implementations.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
class ConnectionStatsTest {

   public static void testSingleLocalConnection(String jmxDomain, String serverName)
           throws MalformedObjectNameException, AttributeNotFoundException, MBeanException, ReflectionException,
           InstanceNotFoundException {
      MBeanServer mbeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName on = new ObjectName(String.format("%s:type=Server,name=%s,component=Transport", jmxDomain, serverName));
      // Now verify that via JMX as well, these stats are also as expected
      assertTrue(Integer.parseInt(mbeanServer.getAttribute(on, "TotalBytesRead").toString()) > 0);
      assertTrue(Integer.parseInt(mbeanServer.getAttribute(on, "TotalBytesWritten").toString()) > 0);
      assertEquals(mbeanServer.getAttribute(on, "NumberOfLocalConnections"), 1);
   }

   public static void testMultipleLocalConnections(String jmxDomain, String serverName, int expectedTotalConns)
           throws MalformedObjectNameException, AttributeNotFoundException, MBeanException, ReflectionException,
           InstanceNotFoundException {
      MBeanServer mbeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName on = new ObjectName(String.format("%s:type=Server,name=%s,component=Transport", jmxDomain, serverName));

      assertEquals(mbeanServer.getAttribute(on, "NumberOfLocalConnections"), expectedTotalConns);
   }

   public static void testGlobalConnections(String jmxDomain, String serverName, int expectedTotalConns,
           MBeanServer mbeanServer) throws MalformedObjectNameException, AttributeNotFoundException, MBeanException,
           ReflectionException, InstanceNotFoundException {
      ObjectName on = new ObjectName(String.format("%s:type=Server,name=%s,component=Transport", jmxDomain, serverName));
      // Now verify that via JMX as well, these stats are also as expected
      assertEquals(mbeanServer.getAttribute(on, "NumberOfGlobalConnections"), expectedTotalConns);
   }

}
