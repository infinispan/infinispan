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

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.InvalidTransactionException;
import org.infinispan.util.AnyEquivalence;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.Equivalence;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;

/**
 * Defines the state of a remotely originated transaction.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class RemoteTransaction extends AbstractCacheTransaction implements Cloneable {

   private static final Log log = LogFactory.getLog(RemoteTransaction.class);

   /**
    * This flag can only be true for transactions received via state transfer. During state transfer we do not migrate
    * lookedUpEntries to save bandwidth. If missingLookedUpEntries is true at the time a CommitCommand is received this
    * indicates the preceding PrepareCommand was received by previous owner before state transfer but not by the
    * current owner which now has to re-execute prepare to populate lookedUpEntries (and acquire the locks).
    */
   private volatile boolean missingLookedUpEntries = false;

   private volatile TotalOrderRemoteTransactionState transactionState;
   private final Object transactionStateLock = new Object();

   public RemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx, int topologyId, Equivalence<Object> keyEquivalence) {
      super(tx, topologyId, keyEquivalence);
      this.modifications = modifications == null || modifications.length == 0
            ? InfinispanCollections.<WriteCommand>emptyList()
            : Arrays.asList(modifications);
      lookedUpEntries = CollectionFactory.makeMap(
            this.modifications.size(), keyEquivalence, AnyEquivalence.<CacheEntry>getInstance());
   }

   public RemoteTransaction(GlobalTransaction tx, int topologyId, Equivalence<Object> keyEquivalence) {
      super(tx, topologyId, keyEquivalence);
      this.modifications = new LinkedList<WriteCommand>();
      lookedUpEntries = CollectionFactory.makeMap(2, keyEquivalence, AnyEquivalence.<CacheEntry>getInstance());
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      checkIfRolledBack();
      if (log.isTraceEnabled()) {
         log.tracef("Adding key %s to tx %s", key, getGlobalTransaction());
      }
      lookedUpEntries.put(key, e);
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> entries) {
      checkIfRolledBack();
      if (log.isTraceEnabled()) {
         log.tracef("Adding keys %s to tx %s", entries.keySet(), getGlobalTransaction());
      }
      lookedUpEntries.putAll(entries);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof RemoteTransaction)) return false;
      RemoteTransaction that = (RemoteTransaction) o;
      return tx.equals(that.tx);
   }

   @Override
   public int hashCode() {
      return tx.hashCode();
   }

   @Override
   public Object clone() {
      try {
         RemoteTransaction dolly = (RemoteTransaction) super.clone();
         dolly.modifications = new ArrayList<WriteCommand>(modifications);
         dolly.lookedUpEntries = CollectionFactory.makeMap(
               lookedUpEntries, keyEquivalence, AnyEquivalence.<CacheEntry>getInstance());
         return dolly;
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!!");
      }
   }

   @Override
   public String toString() {
      return "RemoteTransaction{" +
            "modifications=" + modifications +
            ", lookedUpEntries=" + lookedUpEntries +
            ", lockedKeys=" + lockedKeys +
            ", backupKeyLocks=" + backupKeyLocks +
            ", missingLookedUpEntries=" + missingLookedUpEntries +
            ", isMarkedForRollback=" + isMarkedForRollback()+
            ", tx=" + tx +
            ", state=" + transactionState +
            '}';
   }

   public void setMissingLookedUpEntries(boolean missingLookedUpEntries) {
      this.missingLookedUpEntries = missingLookedUpEntries;
   }

   public boolean isMissingLookedUpEntries() {
      return missingLookedUpEntries;
   }

   private void checkIfRolledBack() {
      if (isMarkedForRollback()) {
         throw new InvalidTransactionException("This remote transaction " + getGlobalTransaction() + " is already rolled back");
      }
   }

   /**
    * @return  get (or create if needed) the {@code TotalOrderRemoteTransactionState} associated to this remote transaction
    */
   public final TotalOrderRemoteTransactionState getTransactionState() {
      if (transactionState != null) {
         return transactionState;
      }
      synchronized (transactionStateLock) {
         if (transactionState == null) {
            transactionState = new TotalOrderRemoteTransactionState(getGlobalTransaction());
         }
         return transactionState;
      }
   }
}
