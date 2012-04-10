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
package org.infinispan.cdi.event.cachemanager;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;

import javax.enterprise.event.Event;

/**
 * @author Pete Muir
 */
@Listener
public class CacheStoppedAdapter extends AbstractAdapter<CacheStoppedEvent> {

   public static final CacheStoppedEvent EMPTY = new CacheStoppedEvent() {

      @Override
      public Type getType() {
         return null;
      }

      @Override
      public EmbeddedCacheManager getCacheManager() {
         return null;
      }

      @Override
      public String getCacheName() {
         return null;
      }
   };

   private final String cacheName;

   public CacheStoppedAdapter(Event<CacheStoppedEvent> event, String cacheName) {
      super(event);
      this.cacheName = cacheName;
   }

   @Override
   @CacheStopped
   public void fire(CacheStoppedEvent payload) {
      if (payload.getCacheName().equals(cacheName)) {
         super.fire(payload);
      }
   }
}
