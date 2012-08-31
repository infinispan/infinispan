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

package org.infinispan.transaction;

import org.infinispan.CacheException;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Object that holds transaction's state on the node where it originated; as opposed to {@link RemoteTransaction}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public abstract class LocalTransaction extends AbstractCacheTransaction {

   private static final Log log = LogFactory.getLog(LocalTransaction.class);
   private static final boolean trace = log.isTraceEnabled();

   private Set<Address> remoteLockedNodes;
   private Set<Object> readKeys = null;

   /** mark as volatile as this might be set from the tx thread code on view change*/
   private volatile boolean isMarkedForRollback;

   private final Transaction transaction;

   private final boolean implicitTransaction;

   public LocalTransaction(Transaction transaction, GlobalTransaction tx, boolean implicitTransaction, int viewId) {
      super(tx, viewId);
      this.transaction = transaction;
      this.implicitTransaction = implicitTransaction;
   }

   public void addModification(WriteCommand mod) {
      if (trace) log.tracef("Adding modification %s. Mod list is %s", mod, modifications);
      if (modifications == null) {
         // we need to synchronize this collection to be able to get a valid snapshot from another thread during state transfer
         modifications = Collections.synchronizedList(new LinkedList<WriteCommand>());
      }
      modifications.add(mod);
   }

   public void locksAcquired(Collection<Address> nodes) {
      if (trace) log.tracef("Adding remote locks on %s. Remote locks are %s", nodes, remoteLockedNodes);
      if (remoteLockedNodes == null)
         remoteLockedNodes = new HashSet<Address>(nodes);
      else
         remoteLockedNodes.addAll(nodes);
   }

   public Collection<Address> getRemoteLocksAcquired(){
	   if (remoteLockedNodes == null) return Collections.emptySet();
	   return remoteLockedNodes;
   }

   public void clearRemoteLocksAcquired() {
      if (remoteLockedNodes != null) remoteLockedNodes.clear();
   }

   public void markForRollback(boolean markForRollback) {
      isMarkedForRollback = markForRollback;
   }

   public final boolean isMarkedForRollback() {
      return isMarkedForRollback;
   }

   public Transaction getTransaction() {
      return transaction;
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return (Map<Object, CacheEntry>)
            (lookedUpEntries == null ? Collections.emptyMap() : lookedUpEntries);
   }

   public boolean isImplicitTransaction() {
      return implicitTransaction;
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      if (isMarkedForRollback()) {
         throw new CacheException("This transaction is marked for rollback and cannot acquire locks!");
      }
      if (lookedUpEntries == null) lookedUpEntries = new HashMap<Object, CacheEntry>(4);
      lookedUpEntries.put(key, e);
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> entries) {
      if (isMarkedForRollback()) {
         throw new CacheException("This transaction is marked for rollback and cannot acquire locks!");
      }
      if (lookedUpEntries == null) {
         lookedUpEntries = new HashMap<Object, CacheEntry>(entries);
      } else {
         lookedUpEntries.putAll(entries);
      }
   }

   public boolean isReadOnly() {
      return (modifications == null || modifications.isEmpty()) && (lookedUpEntries == null || lookedUpEntries.isEmpty());
   }

   public abstract boolean isEnlisted();

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LocalTransaction that = (LocalTransaction) o;

      return tx.getId() == that.tx.getId();
   }

   @Override
   public int hashCode() {
      long id = tx.getId();
      return (int)(id ^ (id >>> 32));
   }

   @Override
   public String toString() {
      return "LocalTransaction{" +
            "remoteLockedNodes=" + remoteLockedNodes +
            ", isMarkedForRollback=" + isMarkedForRollback +
            ", lockedKeys=" + lockedKeys +
            ", backupKeyLocks=" + backupKeyLocks +
            ", viewId=" + viewId +
            "} " + super.toString();
   }

   public void setModifications(List<WriteCommand> modifications) {
      this.modifications = modifications;
   }

   @Override
   public void addReadKey(Object key) {
      if (readKeys == null) readKeys = new HashSet<Object>(2);
      readKeys.add(key);
   }

   @Override
   public boolean keyRead(Object key) {
      return readKeys != null && readKeys.contains(key);
   }
}
