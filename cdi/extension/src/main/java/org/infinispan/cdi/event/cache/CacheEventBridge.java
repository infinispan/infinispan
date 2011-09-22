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
package org.infinispan.cdi.event.cache;

import org.infinispan.cdi.event.AbstractEventBridge;
import org.infinispan.notifications.Listenable;
import org.infinispan.notifications.cachelistener.event.Event;

import java.lang.annotation.Annotation;
import java.util.Set;

/**
 * @author Pete Muir
 */
public class CacheEventBridge extends AbstractEventBridge<Event<?, ?>> {

   public void registerObservers(Set<Annotation> qualifierSet,
                                 Listenable listenable) {
      Annotation[] qualifiers = qualifierSet
            .toArray(new Annotation[qualifierSet.size()]);
      if (hasObservers(CacheEntryActivatedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryActivatedAdapter(getBaseEvent()
                                                                     .select(CacheEntryActivatedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryCreatedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryCreatedAdapter(getBaseEvent()
                                                                   .select(CacheEntryCreatedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryEvictedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryEvictedAdapter(getBaseEvent()
                                                                   .select(CacheEntryEvictedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryInvalidatedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryInvalidatedAdapter(getBaseEvent()
                                                                       .select(CacheEntryInvalidatedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryLoadedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryLoadedAdapter(getBaseEvent()
                                                                  .select(CacheEntryLoadedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryModifiedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryModifiedAdapter(getBaseEvent()
                                                                    .select(CacheEntryModifiedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryPassivatedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryPassivatedAdapter(getBaseEvent()
                                                                      .select(CacheEntryPassivatedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryRemovedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryRemovedAdapter(getBaseEvent()
                                                                   .select(CacheEntryRemovedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryVisitedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryVisitedAdapter(getBaseEvent()
                                                                   .select(CacheEntryVisitedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(TransactionCompletedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new TransactionCompletedAdapter(getBaseEvent()
                                                                      .select(TransactionCompletedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(TransactionRegisteredAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new TransactionRegisteredAdapter(getBaseEvent()
                                                                       .select(TransactionRegisteredAdapter.WILDCARD_TYPE, qualifiers)));
      }
   }
}
