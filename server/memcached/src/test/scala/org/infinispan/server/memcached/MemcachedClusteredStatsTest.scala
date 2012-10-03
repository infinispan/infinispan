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

package org.infinispan.server.memcached

import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.server.core.ConnectionStatsTests._
import org.testng.annotations.Test
import javax.management.{MBeanServer, MBeanServerFactory}
import org.infinispan.jmx.MBeanServerLookup
import java.util.Properties

/**
 * Tests whether statistics of clustered Memcached instances
 * are calculated correctly.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = Array("functional"), testName = "server.memcached.MemcachedClusteredStatsTest")
class MemcachedClusteredStatsTest extends MemcachedMultiNodeTest {

   private val jmxDomain = classOf[MemcachedClusteredStatsTest].getSimpleName

   private val mbeanServerLookup = new ProvidedMBeanServerLookup(
      MBeanServerFactory.createMBeanServer)

   protected def createCacheManager(index: Int): EmbeddedCacheManager = {
      val builder = new ConfigurationBuilder
      builder.clustering().cacheMode(CacheMode.REPL_SYNC)
      // Per-thread mbean server won't work here because the registration will
      // happen in the 'main' thread and the remote call will try to resolve it
      // in a lookup instance associated with an 'OOB-' thread.
      TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain(
         jmxDomain + "-" + index, true, builder, mbeanServerLookup)
   }

   def testSingleConnectionPerServer() {
      testGlobalConnections(jmxDomain + "-0", "Memcached", 2,
         mbeanServerLookup.getMBeanServer(null))
   }

   class ProvidedMBeanServerLookup(mbeanServer: MBeanServer) extends MBeanServerLookup {
      def getMBeanServer(properties: Properties) = mbeanServer
   }

}
