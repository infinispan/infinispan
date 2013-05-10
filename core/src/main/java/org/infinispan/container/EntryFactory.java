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
package org.infinispan.container;

import org.infinispan.atomic.Delta;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;

/**
 * A factory for constructing {@link org.infinispan.container.entries.MVCCEntry} instances for use in the {@link org.infinispan.context.InvocationContext}.
 * Implementations of this interface would typically wrap an internal {@link org.infinispan.container.entries.CacheEntry}
 * with an {@link org.infinispan.container.entries.MVCCEntry}, optionally acquiring the necessary locks via the
 * {@link org.infinispan.util.concurrent.locks.LockManager}.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface EntryFactory {

   /**
    * Wraps an entry for reading.  Usually this is just a raw {@link CacheEntry} but certain combinations of isolation
    * levels and the presence of an ongoing JTA transaction may force this to be a proper, wrapped MVCCEntry.  The entry
    * is also typically placed in the invocation context.
    *
    * @param ctx current invocation context
    * @param key key to look up and wrap
    * @throws InterruptedException when things go wrong, usually trying to acquire a lock
    */
   CacheEntry wrapEntryForReading(InvocationContext ctx, Object key) throws InterruptedException;

   /**
    * Used for wrapping individual keys when clearing the cache. The wrapped entry is added to the
    * supplied InvocationContext.
    */
   MVCCEntry wrapEntryForClear(InvocationContext ctx, Object key) throws InterruptedException;

   /**
    * Used for wrapping a cache entry for replacement. The wrapped entry is added to the
    * supplied InvocationContext.
    */
   MVCCEntry wrapEntryForReplace(InvocationContext ctx, ReplaceCommand cmd) throws InterruptedException;

   /**
    * Used for wrapping a cache entry for removal. The wrapped entry is added to the
    * supplied InvocationContext.
    */
   MVCCEntry wrapEntryForRemove(InvocationContext ctx, Object key) throws InterruptedException;
   
   /**
    * Used for wrapping Delta entry to be applied to DeltaAware object stored in cache. The wrapped
    * entry is added to the supplied InvocationContext.
    */
   CacheEntry wrapEntryForDelta(InvocationContext ctx, Object deltaKey, Delta delta) throws InterruptedException;

   /**
    * Used for wrapping a cache entry for addition to cache. The wrapped entry is added to the
    * supplied InvocationContext.
    */
   MVCCEntry wrapEntryForPut(InvocationContext ctx, Object key, InternalCacheEntry ice,
         boolean undeleteIfNeeded, FlagAffectedCommand cmd) throws InterruptedException;
}
