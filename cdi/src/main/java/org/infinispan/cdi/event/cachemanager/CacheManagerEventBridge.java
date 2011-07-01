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

import org.infinispan.cdi.event.AbstractEventBridge;
import org.infinispan.notifications.Listenable;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;

import java.lang.annotation.Annotation;
import java.util.Set;

/**
 * @author Pete Muir
 */
public class CacheManagerEventBridge extends AbstractEventBridge<Event> {

   public void registerObservers(Set<Annotation> qualifierSet,
                                 String cacheName, Listenable listenable) {
      Annotation[] qualifiers = qualifierSet
            .toArray(new Annotation[qualifierSet.size()]);
      if (hasObservers(CacheStartedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheStartedAdapter(getBaseEvent().select(
               CacheStartedEvent.class, qualifiers), cacheName));
      }
      if (hasObservers(CacheStoppedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheStoppedAdapter(getBaseEvent().select(
               CacheStoppedEvent.class, qualifiers), cacheName));
      }
      if (hasObservers(ViewChangedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new ViewChangedAdapter(getBaseEvent().select(
               ViewChangedEvent.class, qualifiers)));
      }
   }
}
