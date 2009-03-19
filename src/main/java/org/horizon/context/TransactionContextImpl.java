/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.horizon.context;

import org.horizon.commands.write.WriteCommand;
import org.horizon.container.MVCCEntry;
import org.horizon.transaction.GlobalTransaction;
import org.horizon.util.BidirectionalLinkedHashMap;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A transaction context specially geared to dealing with MVCC.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 1.0
 */
public class TransactionContextImpl extends AbstractContext implements TransactionContext {
   /**
    * Local transaction
    */
   private Transaction ltx = null;

   /**
    * List&lt;VisitableCommand&gt; of modifications. They will be replicated on TX commit
    */
   private List<WriteCommand> modificationList;
   /**
    * A list of modifications that have been encountered with a LOCAL mode option.  These will be removed from the
    * modification list during replication.
    */
   private List<WriteCommand> localModifications;

   /**
    * A list of dummy uninitialised entries created by the cache loader interceptor to load data for a given entry in
    * this tx.
    */
   private List<Object> dummyEntriesCreatedByCacheLoader;

   /**
    * List<Object> of keys that have been removed by the transaction
    */
   private List<Object> removedKeys = null;

   private GlobalTransaction gtx;

   protected final int getLockSetSize() {
      // always initialize the lock collection to 8 entries
      return 8;
   }

   public TransactionContextImpl(Transaction tx) throws SystemException, RollbackException {
      ltx = tx;
      lookedUpEntries = new BidirectionalLinkedHashMap<Object, MVCCEntry>(8);
   }

   public void reset() {
      super.reset();
      modificationList = null;
      localModifications = null;
      if (dummyEntriesCreatedByCacheLoader != null) dummyEntriesCreatedByCacheLoader.clear();
      if (removedKeys != null) removedKeys.clear();
      lookedUpEntries = new BidirectionalLinkedHashMap<Object, MVCCEntry>(8);
   }

   public GlobalTransaction getGobalTransaction() {
      return gtx;
   }

   public void putLookedUpEntries(Map<Object, MVCCEntry> entries) {
      lookedUpEntries.putAll(entries);
   }

   public void addModification(WriteCommand command) {
      if (command == null) return;
      if (modificationList == null) modificationList = new LinkedList<WriteCommand>();
      modificationList.add(command);
   }

   public List<WriteCommand> getModifications() {
      if (modificationList == null) return Collections.emptyList();
      return modificationList;
   }

   public void addLocalModification(WriteCommand command) {
      if (command == null) throw new NullPointerException("Command is null!");
      if (localModifications == null) localModifications = new LinkedList<WriteCommand>();
      localModifications.add(command);
   }

   public List<WriteCommand> getLocalModifications() {
      if (localModifications == null) return Collections.emptyList();
      return localModifications;
   }


   public void addRemovedEntry(Object key) {
      if (key == null) throw new NullPointerException("Key is null!");
      if (removedKeys == null) removedKeys = new LinkedList<Object>();
      removedKeys.add(key);
   }

   public List<Object> getRemovedEntries() {
      if (removedKeys == null) return Collections.emptyList();
      return new ArrayList<Object>(removedKeys);
   }

   public void setTransaction(Transaction tx) {
      ltx = tx;
   }

   public void setGlobalTransaction(GlobalTransaction gtx) {
      this.gtx = gtx;
   }

   public Transaction getTransaction() {
      return ltx;
   }

   public boolean isForceAsyncReplication() {
      return isFlagSet(ContextFlags.FORCE_ASYNCHRONOUS);
   }

   public void setForceAsyncReplication(boolean forceAsyncReplication) {
      setFlag(ContextFlags.FORCE_ASYNCHRONOUS, forceAsyncReplication);
      if (forceAsyncReplication) unsetFlag(ContextFlags.FORCE_SYNCHRONOUS);
   }

   public boolean isForceSyncReplication() {
      return isFlagSet(ContextFlags.FORCE_SYNCHRONOUS);
   }

   public void setForceSyncReplication(boolean forceSyncReplication) {
      setFlag(ContextFlags.FORCE_SYNCHRONOUS, forceSyncReplication);
      if (forceSyncReplication) unsetFlag(ContextFlags.FORCE_ASYNCHRONOUS);
   }

   /**
    * Returns debug information about this transaction.
    */
   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("TransactionContext (").append(System.identityHashCode(this)).append(") nmodificationList: ").append(modificationList);
      return sb.toString();
   }

   public void addDummyEntryCreatedByCacheLoader(Object key) {
      if (dummyEntriesCreatedByCacheLoader == null)
         dummyEntriesCreatedByCacheLoader = new LinkedList<Object>();
      dummyEntriesCreatedByCacheLoader.add(key);
   }

   public List<Object> getDummyEntriesCreatedByCacheLoader() {
      if (dummyEntriesCreatedByCacheLoader == null) return Collections.emptyList();
      return dummyEntriesCreatedByCacheLoader;
   }

   public boolean hasModifications() {
      return modificationList != null && !modificationList.isEmpty();
   }

   public boolean hasLocalModifications() {
      return localModifications != null && !localModifications.isEmpty();
   }

   public boolean hasAnyModifications() {
      return hasModifications() || hasLocalModifications();
   }

//   public ReversibleOrderedSet<Object> getKeysLocked() {
//      return locks == null ? HorizonCollections.emptyReversibleOrderedSet() : Immutables.immutableReversibleOrderedSetCopy(locks);
//   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      TransactionContextImpl that = (TransactionContextImpl) o;

      if (dummyEntriesCreatedByCacheLoader != null ? !dummyEntriesCreatedByCacheLoader.equals(that.dummyEntriesCreatedByCacheLoader) : that.dummyEntriesCreatedByCacheLoader != null)
         return false;
      if (gtx != null ? !gtx.equals(that.gtx) : that.gtx != null) return false;
      if (localModifications != null ? !localModifications.equals(that.localModifications) : that.localModifications != null)
         return false;
      if (ltx != null ? !ltx.equals(that.ltx) : that.ltx != null) return false;
      if (modificationList != null ? !modificationList.equals(that.modificationList) : that.modificationList != null)
         return false;
      if (removedKeys != null ? !removedKeys.equals(that.removedKeys) : that.removedKeys != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (ltx != null ? ltx.hashCode() : 0);
      result = 31 * result + (modificationList != null ? modificationList.hashCode() : 0);
      result = 31 * result + (localModifications != null ? localModifications.hashCode() : 0);
      result = 31 * result + (dummyEntriesCreatedByCacheLoader != null ? dummyEntriesCreatedByCacheLoader.hashCode() : 0);
      result = 31 * result + (removedKeys != null ? removedKeys.hashCode() : 0);
      result = 31 * result + (gtx != null ? gtx.hashCode() : 0);
      return result;
   }
}
