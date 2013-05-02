/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.jcache.interceptor;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jcache.JCacheNotifier;
import org.infinispan.util.TimeService;

import javax.cache.Cache;

/**
 * An interceptor that tracks expiration of entries and notifies JCache
 * {@link javax.cache.event.CacheEntryExpiredListener} instances.
 *
 * This interceptor must be placed before
 * {@link org.infinispan.interceptors.EntryWrappingInterceptor} because this
 * interceptor can result in container entries being removed upon expiration
 * (alongside their metadata).
 *
 * TODO: How to track expired entry in cache stores?
 * TODO: Could this be used as starting point to centrally track expiration?
 * Currently, logic split between data container, cache stores...etc.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class ExpirationTrackingInterceptor extends CommandInterceptor {

   private final DataContainer container;
   private final Cache<Object, Object> cache;
   private final JCacheNotifier<Object, Object> notifier;
   private final TimeService timeService;

   @SuppressWarnings("unchecked")
   public ExpirationTrackingInterceptor(DataContainer container,
         Cache<?, ?> cache, JCacheNotifier<?, ?> notifier, TimeService timeService) {
      this.container = container;
      this.timeService = timeService;
      this.cache = (Cache<Object, Object>) cache;
      this.notifier = (JCacheNotifier<Object, Object>) notifier;
   }

   @Override
   public Object visitGetKeyValueCommand
         (InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      InternalCacheEntry entry = container.peek(key);
      if (entry != null && entry.canExpire() && entry.isExpired(timeService.wallClockTime()))
         notifier.notifyEntryExpired(cache, key, entry.getValue());

      return super.visitGetKeyValueCommand(ctx, command);
   }

   // TODO: Implement any other visitX methods?

}
