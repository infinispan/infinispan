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

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.concurrent.TimeoutException;

/**
 * A factory for constructing {@link org.infinispan.container.entries.MVCCEntry} instances for use in the {@link org.infinispan.context.InvocationContext}.
 * Implementations of this interface would typically wrap an internal {@link org.infinispan.container.entries.CacheEntry}
 * with an {@link org.infinispan.container.entries.MVCCEntry}, optionally acquiring the necessary locks via the
 * {@link org.infinispan.util.concurrent.locks.LockManager}.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface EntryFactory {

   void releaseLock(InvocationContext ctx, Object key);

   /**
    * Attempts to lock an entry if the lock isn't already held in the current scope, and records the lock in the
    * context.
    *
    * @param ctx context
    * @param key Key to lock
    * @return true if a lock was needed and acquired, false if it didn't need to acquire the lock (i.e., lock was
    *         already held)
    * @throws InterruptedException if interrupted
    * @throws org.infinispan.util.concurrent.TimeoutException
    *                              if we are unable to acquire the lock after a specified timeout.
    */
   boolean acquireLock(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException;

   /**
    * Wraps an entry for writing.  This would typically acquire write locks if necessary, and place the wrapped
    * entry in the invocation context.
    *
    * @param ctx current invocation context
    * @param key key to look up and wrap
    * @param createIfAbsent if true, an entry is created if it does not exist in the data container.
    * @param forceLockIfAbsent forces a lock even if the entry is absent
    * @param alreadyLocked if true, this hint prevents the method from acquiring any locks and the existence and ownership of the lock is presumed.
    * @param forRemoval if true, this hint informs this method that the lock is being acquired for removal.
    * @param undeleteIfNeeded if true, if the entry is found in the current scope (perhaps a transaction) and is deleted, it will be undeleted.  If false, it will be considered deleted.
    * @return an MVCCEntry instance
    * @throws InterruptedException when things go wrong, usually trying to acquire a lock
    */
   MVCCEntry wrapEntryForWriting(InvocationContext ctx, Object key, boolean createIfAbsent, boolean forceLockIfAbsent, boolean alreadyLocked, boolean forRemoval, boolean undeleteIfNeeded) throws InterruptedException;

   /**
    * Wraps an entry for writing.  This would typically acquire write locks if necessary, and place the wrapped
    * entry in the invocation context.
    *
    * @param ctx current invocation context
    * @param entry an internal entry to wrap
    * @param createIfAbsent if true, an entry is created if it does not exist in the data container.
    * @param forceLockIfAbsent forces a lock even if the entry is absent
    * @param alreadyLocked if true, this hint prevents the method from acquiring any locks and the existence and ownership of the lock is presumed.
    * @param forRemoval if true, this hint informs this method that the lock is being acquired for removal.
* @param undeleteIfNeeded if true, if the entry is found in the current scope (perhaps a transaction) and is deleted, it will be undeleted.  If false, it will be considered deleted.    * @return an MVCCEntry instance
    * @throws InterruptedException when things go wrong, usually trying to acquire a lock
    */
   MVCCEntry wrapEntryForWriting(InvocationContext ctx, InternalCacheEntry entry, boolean createIfAbsent, boolean forceLockIfAbsent, boolean alreadyLocked, boolean forRemoval, boolean undeleteIfNeeded) throws InterruptedException;

   /**
    * Wraps an entry for reading.  Usually this is just a raw {@link CacheEntry} but certain combinations of isolation
    * levels and the presence of an ongoing JTA transaction may force this to be a proper, wrapped MVCCEntry.  The entry
    * is also typically placed in the invocation context.
    *
    * @param ctx current invocation context
    * @param key key to look up and wrap
    * @return an entry for reading
    * @throws InterruptedException when things go wrong, usually trying to acquire a lock
    */
   CacheEntry wrapEntryForReading(InvocationContext ctx, Object key) throws InterruptedException;
}
