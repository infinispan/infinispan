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
package org.infinispan.transaction.xa;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Defines the state a infinispan transaction should have.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface CacheTransaction {

   /**
    * Returns the transaction identifier.
    */
   GlobalTransaction getGlobalTransaction();

   /**
    * Returns the modifications visible within the current transaction.
    */
   List<WriteCommand> getModifications();


   CacheEntry lookupEntry(Object key);

   Map<Object, CacheEntry> getLookedUpEntries();

   void putLookedUpEntry(Object key, CacheEntry e);

   void putLookedUpEntries(Map<Object, CacheEntry> entries);

   void removeLookedUpEntry(Object key);

   void clearLookedUpEntries();

   boolean ownsLock(Object key);
   
   void clearLockedKeys();

   Set<Object> getLockedKeys();

   int getTopologyId();

   Set<Object> getBackupLockedKeys();

   void addBackupLockForKey(Object key);

   /**
    * @see org.infinispan.interceptors.locking.AbstractTxLockingInterceptor#lockKeyAndCheckOwnership(org.infinispan.context.InvocationContext, Object)
    */
   void notifyOnTransactionFinished();

   /**
    * Checks if this transaction holds a lock on the given key and then waits until the transaction completes or until
    * the timeout expires and returns <code>true</code> if the transaction is complete or <code>false</code> otherwise.
    * If the key is not locked or if the transaction is already completed it returns <code>true</code> immediately.
    * <p/>
    * This method is subject to spurious returns in a way similar to {@link java.lang.Object#wait()}. It can sometimes return
    * before the specified time has elapsed and without guaranteeing that this transaction is complete. The caller is
    * responsible to call the method again if transaction completion was not reached and the time budget was not spent.
    *
    * @see org.infinispan.interceptors.locking.AbstractTxLockingInterceptor#lockKeyAndCheckOwnership(org.infinispan.context.InvocationContext, Object)
    */
   boolean waitForLockRelease(Object key, long lockAcquisitionTimeout) throws InterruptedException;

   EntryVersionsMap getUpdatedEntryVersions();

   void setUpdatedEntryVersions(EntryVersionsMap updatedEntryVersions);

   boolean keyRead(Object key);

   void addReadKey(Object key);
}
