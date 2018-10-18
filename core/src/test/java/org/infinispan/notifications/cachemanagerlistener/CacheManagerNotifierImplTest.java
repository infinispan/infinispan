package org.infinispan.notifications.cachemanagerlistener;

import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;

import org.infinispan.factories.KnownComponentNames;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.WithinThreadExecutor;
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

      TestingUtil.inject(n, TestingUtil.named(KnownComponentNames.ASYNC_NOTIFICATION_EXECUTOR, new WithinThreadExecutor()));

      n.start();
      n.addListener(cl);
   }

   public void testNotifyViewChanged() {
      Address a = mock(Address.class);
      List<Address> addresses = Collections.emptyList();
      n.notifyViewChange(addresses, addresses, a, 100);

      assert cl.invocationCount == 1;
      assert ((ViewChangedEvent) cl.getEvent()).getLocalAddress() == a;
      assert ((ViewChangedEvent) cl.getEvent()).getNewMembers() == addresses;
      assert ((ViewChangedEvent) cl.getEvent()).getViewId() == 100;
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
