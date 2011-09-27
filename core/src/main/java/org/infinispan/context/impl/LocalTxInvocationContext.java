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
package org.infinispan.context.impl;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.AbstractCacheTransaction;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.BidirectionalLinkedHashMap;
import org.infinispan.util.BidirectionalMap;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Invocation context to be used for locally originated transactions.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public class LocalTxInvocationContext extends AbstractTxInvocationContext {

   public static final BidirectionalLinkedHashMap<Object,CacheEntry> EMPTY_ENTRY_MAP = new BidirectionalLinkedHashMap<Object, CacheEntry>();

   private LocalTransaction localTransaction;

   private boolean replayEntryWrapping;

   public boolean isTransactionValid() {
      Transaction t = getTransaction();
      int status = -1;
      if (t != null) {
         try {
            status = t.getStatus();
         } catch (SystemException e) {
            // no op
         }
      }
      return status == Status.STATUS_ACTIVE || status == Status.STATUS_PREPARING;
   }

   public boolean isOriginLocal() {
      return true;
   }

   public Object getLockOwner() {
      return localTransaction.getGlobalTransaction();
   }

   public GlobalTransaction getGlobalTransaction() {
      return localTransaction.getGlobalTransaction();
   }

   public List<WriteCommand> getModifications() {
      return localTransaction == null ? null : localTransaction.getModifications();
   }

   public void setLocalTransaction(LocalTransaction localTransaction) {
      this.localTransaction = localTransaction;
   }

   public CacheEntry lookupEntry(Object key) {
      return localTransaction != null ? localTransaction.lookupEntry(key) : null;
   }

   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
      return localTransaction != null ? localTransaction.getLookedUpEntries() : EMPTY_ENTRY_MAP;
   }

   public void putLookedUpEntry(Object key, CacheEntry e) {
      localTransaction.putLookedUpEntry(key, e);
   }

   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      for (Map.Entry<Object, CacheEntry> ce : lookedUpEntries.entrySet()) {
         localTransaction.putLookedUpEntry(ce.getKey(), ce.getValue());
      }
   }

   public void removeLookedUpEntry(Object key) {
      localTransaction.removeLookedUpEntry(key);
   }

   public void clearLookedUpEntries() {
      localTransaction.clearLookedUpEntries();
   }

   @Override
   public boolean hasLockedKey(Object key) {
      return localTransaction != null && localTransaction.ownsLock(key);
   }

   public void remoteLocksAcquired(Collection<Address> nodes) {
      localTransaction.locksAcquired(nodes);
   }

   public Collection<Address> getRemoteLocksAcquired() {
      return localTransaction.getRemoteLocksAcquired();
   }

   @Override
   public AbstractCacheTransaction getCacheTrasaction() {
      return localTransaction;
   }

   @Override
   public Set<Object> getLockedKeys() {
      return localTransaction == null ? null : localTransaction.getLockedKeys();
   }

   @Override
   public void registerLockedKey(Object key) {
      localTransaction.registerLockedKey(key);
   }

   @Override
   public final boolean isReplayEntryWrapping() {
      return replayEntryWrapping;
   }

   public final void setReplayEntryWrapping(boolean replayEntryWrapping) {
      this.replayEntryWrapping = replayEntryWrapping;
   }
}
