/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.notifications.cachemanagerlistener;

import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import static org.mockito.Mockito.*;

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
