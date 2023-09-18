package org.infinispan.notifications.cachemanagerlistener;

import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.List;

import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.notifications.cachemanagerlistener.event.SitesViewChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "notifications.cachemanagerlistener.CacheManagerNotifierImplTest")
public class CacheManagerNotifierImplTest extends AbstractInfinispanTest {
   CacheManagerNotifierImpl n;
   CacheManagerListener cl;

   @BeforeMethod
   public void setUp() {
      n = new CacheManagerNotifierImpl();
      cl = new CacheManagerListener();

      n.start();
      n.addListener(cl);
   }

   public void testNotifyViewChanged() {
      Address a = mock(Address.class);
      List<Address> addresses = Collections.emptyList();
      n.notifyViewChange(addresses, addresses, a, 100);

      ViewChangedEvent event = assertSingleEvent(ViewChangedEvent.class, Event.Type.VIEW_CHANGED);
      assertSame(a, event.getLocalAddress());
      assertSame(addresses, event.getNewMembers());
      assertEquals(100, event.getViewId());
   }

   public void testNotifyCacheStarted() {
      n.notifyCacheStarted("cache");

      CacheStartedEvent event = assertSingleEvent(CacheStartedEvent.class, Event.Type.CACHE_STARTED);
      assertEquals("cache", event.getCacheName());
   }

   public void testNotifyCacheStopped() {
      n.notifyCacheStopped("cache");

      CacheStoppedEvent event = assertSingleEvent(CacheStoppedEvent.class, Event.Type.CACHE_STOPPED);
      assertEquals("cache", event.getCacheName());
   }

   public void testNotifySitesViewChanged() {
      n.notifyCrossSiteViewChanged(List.of("a", "b"), List.of("b"), List.of("c"));

      SitesViewChangedEvent event = assertSingleEvent(SitesViewChangedEvent.class, Event.Type.SITES_VIEW_CHANGED);
      assertEquals(List.of("a", "b"), event.getSites());
      assertEquals(List.of("b"), event.getJoiners());
      assertEquals(List.of("c"), event.getLeavers());
   }

   private <T extends Event> T assertSingleEvent(Class<T> eventInterface, Event.Type type) {
      assertEquals(1, cl.getInvocationCount());
      assertEquals(type, cl.getEvent().getType());
      assertTrue(eventInterface.isInstance(cl.getEvent()));
      return eventInterface.cast(cl.getEvent());
   }
}
