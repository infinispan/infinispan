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
package org.infinispan.eviction;

import java.util.Map;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Central component that deals with eviction of cache entries.
 * <p />
 * Typically, {@link #processEviction()} is called periodically by the eviction thread (which can be configured using
 * {@link org.infinispan.config.FluentConfiguration.ExpirationConfig#wakeUpInterval(Long)} and {@link org.infinispan.config.GlobalConfiguration#setEvictionScheduledExecutorFactoryClass(String)}).
 * <p />
 * If the eviction thread is disabled - by setting {@link org.infinispan.config.FluentConfiguration.ExpirationConfig#wakeUpInterval(Long)} to <tt>0</tt> -
 * then this method could be called directly, perhaps by any other maintenance thread that runs periodically in the application.
 * <p />
 * Note that this method is a no-op if the eviction strategy configured is {@link org.infinispan.eviction.EvictionStrategy#NONE}.
 * <p />
 * @author Manik Surtani
 * @since 4.0
 */
@ThreadSafe
@Scope(Scopes.NAMED_CACHE)
public interface EvictionManager {

   /**
    * Processes the eviction event queue.
    */
   void processEviction();

   /**
    * @return true if eviction is enabled, false otherwise
    */
   boolean isEnabled();

   void onEntryEviction(Map<Object, InternalCacheEntry> evicted);
}
