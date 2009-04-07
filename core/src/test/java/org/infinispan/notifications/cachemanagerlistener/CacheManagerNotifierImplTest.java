package org.infinispan.notifications.cachemanagerlistener;

import org.easymock.EasyMock;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

@Test(groups = "unit", testName = "notifications.cachemanagerlistener.CacheManagerNotifierImplTest")
public class CacheManagerNotifierImplTest {
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
      Address a = EasyMock.createNiceMock(Address.class);
      List<Address> addresses = Collections.emptyList();
      n.notifyViewChange(addresses, a);

      assert cl.invocationCount == 1;
      assert ((ViewChangedEvent) cl.getEvent()).getLocalAddress() == a;
      assert ((ViewChangedEvent) cl.getEvent()).getNewMemberList() == addresses;
      assert cl.getEvent().getType() == Event.Type.VIEW_CHANGED;
   }

   public void testNotifyCacheStarted() {
      n.notifyCacheStarted("cache");

      assert cl.invocationCount == 1;
      assert ((CacheStartedEvent) cl.getEvent()).getCacheName().equals("cache");
      assert cl.getEvent().getType() == Event.Type.CACHE_STARTED;
   }

   public void testNotifyCacheStopped() {
      n.notifyCacheStopped("cache");

      assert cl.invocationCount == 1;
      assert ((CacheStoppedEvent) cl.getEvent()).getCacheName().equals("cache");
      assert cl.getEvent().getType() == Event.Type.CACHE_STOPPED;
   }
}
