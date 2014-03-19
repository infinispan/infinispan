package org.infinispan.server.hotrod

import org.testng.annotations.Test
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.remoting.transport.Address
import org.infinispan.notifications.cachemanagerlistener.event.Event.Type
import org.infinispan.distribution.TestAddress
import java.util.ArrayList
import org.testng.AssertJUnit._
import org.infinispan.test.SingleCacheManagerTest
import org.infinispan.notifications.cachemanagerlistener.event.impl.EventImpl

/**
 * Tests crashed or stopped member logic.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.CrashedMemberDetectorTest")
class CrashedMemberDetectorTest extends SingleCacheManagerTest {

   protected def createCacheManager() =
      TestCacheManagerFactory.createCacheManager()

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