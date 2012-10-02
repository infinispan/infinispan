/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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

import org.testng.annotations.Test
import org.infinispan.server.core.Main
import test.MemcachedTestingUtil._
import org.testng.Assert._

@Test(groups = Array("functional"), testName = "server.memcached.MemcachedMainTest")
class MemcachedMainTest {

   def testMainNoConfigExposesStatistics() {
      Main.boot(Array("-r", "memcached", "-p", "23345"))

      try {
         val memcachedClient = createMemcachedClient(60000, 23345)
         val allStats = memcachedClient.getStats
         assertEquals(allStats.size(), 1)
         val stats = allStats.values.iterator.next
         assertEquals(stats.get("cmd_set"), "0")
      } finally {
         Main.getServer.stop
         Main.getCacheManager.stop()
      }
   }
   
}