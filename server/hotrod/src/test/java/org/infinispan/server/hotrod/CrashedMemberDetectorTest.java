package org.infinispan.server.hotrod;

import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachemanagerlistener.event.Event.Type;
import org.infinispan.notifications.cachemanagerlistener.event.impl.EventImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
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
      var address1 = JGroupsAddress.random("a");
      var address2 = JGroupsAddress.random("b");
      var address3 = JGroupsAddress.random("c");

      Cache<Address, ServerAddress> cache = cacheManager.getCache();
      cache.put(address1, ServerAddress.forAddress("a", 123, true));
      cache.put(address2, ServerAddress.forAddress("b", 456, true));
      cache.put(address3, ServerAddress.forAddress("c", 789, true));

      CrashedMemberDetectorListener detector = new CrashedMemberDetectorListener(cache, null);

      List<Address> oldMembers = new ArrayList<>();
      oldMembers.add(JGroupsAddress.random());
      oldMembers.add(JGroupsAddress.random());
      oldMembers.add(JGroupsAddress.random());

      List<Address> newMembers = new ArrayList<>();
      newMembers.add(JGroupsAddress.random());
      newMembers.add(JGroupsAddress.random());

      EventImpl e = new EventImpl("", cacheManager, Type.VIEW_CHANGED, newMembers,
                              oldMembers, address1, 99);

      detector.detectCrashedMember(e);

      assertTrue(cache.containsKey(address1));
      assertTrue(cache.containsKey(address2));
   }

}
