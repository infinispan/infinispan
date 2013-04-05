/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.server.hotrod

import org.testng.annotations.Test
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.remoting.transport.Address
import org.infinispan.notifications.cachemanagerlistener.event.Event.Type
import org.infinispan.distribution.TestAddress
import java.util.ArrayList
import org.testng.AssertJUnit._
import org.infinispan.test.SingleCacheManagerTest
import org.infinispan.notifications.cachemanagerlistener.event.EventImpl

/**
 * Tests crashed or stopped member logic.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.CrashedMemberDetectorTest")
class CrashedMemberDetectorTest extends SingleCacheManagerTest {

   protected def createCacheManager() =
      TestCacheManagerFactory.createLocalCacheManager(false)

   def testDetectCrashedMembers() {
      val cache = cacheManager.getCache[Address, ServerAddress]()
      cache.put(new TestAddress(1), new ServerAddress("a", 123))
      cache.put(new TestAddress(2), new ServerAddress("b", 456))
      cache.put(new TestAddress(3), new ServerAddress("c", 789))

      val detector = new CrashedMemberDetectorListener(cache, null)

      val oldMembers = new ArrayList[Address]()
      oldMembers.add(new TestAddress(1))
      oldMembers.add(new TestAddress(3))
      oldMembers.add(new TestAddress(2))

      val newMembers = new ArrayList[Address]()
      newMembers.add(new TestAddress(1))
      newMembers.add(new TestAddress(2))

      val e = new EventImpl("", cacheManager, Type.VIEW_CHANGED, newMembers,
                            oldMembers, new TestAddress(1), 99)

      detector.detectCrashedMember(e)

      assertTrue(cache.containsKey(new TestAddress(1)))
      assertTrue(cache.containsKey(new TestAddress(2)))
   }

}