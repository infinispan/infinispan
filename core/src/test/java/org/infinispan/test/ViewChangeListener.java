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
package org.infinispan.test;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Listens for view changes.  Note that you do NOT have to register this listener; it does so automatically when
 * constructed.
 */
@Listener
public class ViewChangeListener {
   CacheContainer cm;
   final CountDownLatch latch = new CountDownLatch(1);

   public ViewChangeListener(Cache c) {
      this(c.getCacheManager());
   }

   public ViewChangeListener(EmbeddedCacheManager cm) {
      this.cm = cm;
      cm.addListener(this);
   }

   @ViewChanged
   public void onViewChange(ViewChangedEvent e) {
      latch.countDown();
   }

   /**
    * Blocks for a certain amount of time until a view change is received.  Note that this class will start listening
    * for the view change the moment it is constructed.
    *
    * @param time time to wait
    * @param unit time unit
    */
   public void waitForViewChange(long time, TimeUnit unit) throws InterruptedException {
      if (!latch.await(time, unit)) assert false : "View change not seen after " + time + " " + unit;
   }
}
