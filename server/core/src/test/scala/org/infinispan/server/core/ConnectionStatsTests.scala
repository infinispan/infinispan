/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.server.core

import org.infinispan.jmx.PerThreadMBeanServerLookup
import javax.management.{MBeanServer, ObjectName}
import org.testng.Assert._

/**
 * Connection statistic tests that are common to all
 * Netty-based server implementations.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
object ConnectionStatsTests {

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
