/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.cdi.test.event;

import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

/**
 * @author Pete Muir
 */
@ApplicationScoped
public class Cache2Observers {

   private CacheStartedEvent cacheStartedEvent;
   private int cacheStartedEventCount;

   /**
    * Observe the cache started event for the cache associated with @Cache2
    */
   public void observeCacheStarted(@Observes @Cache2 CacheStartedEvent event) {
      this.cacheStartedEventCount++;
      this.cacheStartedEvent = event;
   }

   public CacheStartedEvent getCacheStartedEvent() {
      return cacheStartedEvent;
   }

   public int getCacheStartedEventCount() {
      return cacheStartedEventCount;
   }

}
