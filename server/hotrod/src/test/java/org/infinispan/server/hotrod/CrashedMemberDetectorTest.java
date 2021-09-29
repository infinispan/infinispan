package org.infinispan.server.hotrod;

import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.TestAddress;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachemanagerlistener.event.Event.Type;
import org.infinispan.notifications.cachemanagerlistener.event.impl.EventImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests crashed or stopped member logic.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "server.hotrod.CrashedMemberDetectorTest")
public class CrashedMemberDetectorTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      return TestCacheManagerFactory.createCacheManager(new ConfigurationBuilder());
   }

   public void testDetectCrashedMembers() {
      Cache<Address, ServerAddress> cache = cacheManager.getCache();
      cache.put(new TestAddress(1), ServerAddress.forAddress("a", 123, true));
      cache.put(new TestAddress(2), ServerAddress.forAddress("b", 456, true));
      cache.put(new TestAddress(3), ServerAddress.forAddress("c", 789, true));

      CrashedMemberDetectorListener detector = new CrashedMemberDetectorListener(cache, null);

      List<Address> oldMembers = new ArrayList<>();
      oldMembers.add(new TestAddress(1));
      oldMembers.add(new TestAddress(3));
      oldMembers.add(new TestAddress(2));

      List<Address> newMembers = new ArrayList<>();
      newMembers.add(new TestAddress(1));
      newMembers.add(new TestAddress(2));

      EventImpl e = new EventImpl("", cacheManager, Type.VIEW_CHANGED, newMembers,
                              oldMembers, new TestAddress(1), 99);

      detector.detectCrashedMember(e);

      assertTrue(cache.containsKey(new TestAddress(1)));
      assertTrue(cache.containsKey(new TestAddress(2)));
   }

}
