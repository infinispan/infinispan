/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.infinispan.context;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.transaction.GlobalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.BidirectionalLinkedHashMap;
import org.infinispan.util.BidirectionalMap;
import org.infinispan.util.HorizonCollections;

import javax.transaction.Transaction;
import java.util.Map;

public class InvocationContextImpl extends AbstractContext implements InvocationContext {
   private Transaction transaction;
   private GlobalTransaction globalTransaction;
   protected volatile TransactionContext transactionContext;

   public InvocationContextImpl() {
      // set this to true by default
      setContextFlag(ContextFlags.ORIGIN_LOCAL);
   }

   protected final int getLockSetSize() {
      // always use 4 for invocation locks
      return 4;
   }

   private void initLookedUpEntries() {
      if (lookedUpEntries == null) lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(4);
   }

   @Override
   public boolean hasLockedKey(Object key) {
      if (transactionContext != null) return transactionContext.hasLockedKey(key);
      return super.hasLockedKey(key);
   }

   @Override
   public CacheEntry lookupEntry(Object k) {
      if (transactionContext != null) {
         return transactionContext.lookupEntry(k);
      } else {
         return lookedUpEntries == null ? null : lookedUpEntries.get(k);
      }
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      if (transactionContext != null) {
         transactionContext.removeLookedUpEntry(key);
      } else {
         if (lookedUpEntries != null) lookedUpEntries.remove(key);
      }
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      if (transactionContext != null)
         transactionContext.putLookedUpEntry(key, e);
      else {
         initLookedUpEntries();
         lookedUpEntries.put(key, e);
      }
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      if (transactionContext != null)
         transactionContext.putLookedUpEntries(lookedUpEntries);
      else {
         initLookedUpEntries();
         this.lookedUpEntries.putAll(lookedUpEntries);
      }
   }

   @Override
   public void clearLookedUpEntries() {
      if (lookedUpEntries != null) lookedUpEntries.clear();
   }

   @SuppressWarnings("unchecked")
   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
      if (transactionContext != null) return transactionContext.getLookedUpEntries();
      return (BidirectionalMap<Object, CacheEntry>)
            (lookedUpEntries == null ? HorizonCollections.emptyBidirectionalMap() : lookedUpEntries);
   }

   @SuppressWarnings("unchecked")
   public InvocationContext copy() {
      InvocationContextImpl copy = new InvocationContextImpl();
      copyInto(copy);
      copy.globalTransaction = globalTransaction;
      copy.transaction = transaction;
      copy.transactionContext = transactionContext;
      return copy;
   }


   /**
    * Marks teh context as only rolling back.
    *
    * @param localRollbackOnly if true, the context is only rolling back.
    */
   public void setLocalRollbackOnly(boolean localRollbackOnly) {
      setContextFlag(ContextFlags.LOCAL_ROLLBACK_ONLY, localRollbackOnly);
   }

   /**
    * Retrieves the transaction associated with this invocation
    *
    * @return The transaction associated with this invocation
    */
   public Transaction getTransaction() {
      return transaction;
   }

   /**
    * Sets a transaction object on the invocation context.
    *
    * @param transaction transaction to set
    */
   public void setTransaction(Transaction transaction) {
      this.transaction = transaction;
   }

   /**
    * @return the transaction entry associated with the current transaction, or null if the current thread is not
    *         associated with a transaction.
    * @since 4.0
    */
   public TransactionContext getTransactionContext() {
      return transactionContext;
   }

   /**
    * Sets the transaction context to be associated with the current thread.
    *
    * @param transactionContext transaction context to set
    * @since 4.0
    */
   public void setTransactionContext(TransactionContext transactionContext) {
      this.transactionContext = transactionContext;
   }

   /**
    * Retrieves the global transaction associated with this invocation
    *
    * @return the global transaction associated with this invocation
    */
   public GlobalTransaction getGlobalTransaction() {
      return globalTransaction;
   }

   /**
    * Sets the global transaction associated with this invocation
    *
    * @param globalTransaction global transaction to set
    */
   public void setGlobalTransaction(GlobalTransaction globalTransaction) {
      this.globalTransaction = globalTransaction;
   }

   /**
    * Tests if this invocation originated locally or from a remote cache.
    *
    * @return true if the invocation originated locally.
    */
   public boolean isOriginLocal() {
      return isContextFlagSet(ContextFlags.ORIGIN_LOCAL);
   }

   /**
    * If set to true, the invocation is assumed to have originated locally.  If set to false, assumed to have originated
    * from a remote cache.
    *
    * @param originLocal flag to set
    */
   public void setOriginLocal(boolean originLocal) {
      setContextFlag(ContextFlags.ORIGIN_LOCAL, originLocal);
   }

   /**
    * @return true if the current transaction is set to rollback only.
    */
   public boolean isLocalRollbackOnly() {
      return isContextFlagSet(ContextFlags.LOCAL_ROLLBACK_ONLY);
   }

   /**
    * Resets the context, freeing up any references.
    */
   public void reset() {
      super.reset();
      transaction = null;
      globalTransaction = null;
      transactionContext = null;
      setContextFlag(ContextFlags.ORIGIN_LOCAL);
      lookedUpEntries = null;
   }

   /**
    * Sets the state of the InvocationContext based on the template context passed in
    *
    * @param template template to copy from
    */
   public void setState(InvocationContext template) {
      if (template == null) {
         throw new NullPointerException("Template InvocationContext passed in to InvocationContext.setState() passed in is null");
      }

      this.setGlobalTransaction(template.getGlobalTransaction());
      this.setLocalRollbackOnly(template.isLocalRollbackOnly());
      this.setFlags(template.getFlags());
      this.setOriginLocal(template.isOriginLocal());
      this.setTransaction(template.getTransaction());
   }

   /**
    * @return true if there is current transaction associated with the invocation, and this transaction is in a valid
    *         state.
    */
   public boolean isValidTransaction() {
      // ought to move to the transaction context
      return transaction != null && TransactionTable.isValid(transaction);
   }

   @Override
   public boolean isContainsModifications() {
      return transactionContext == null ? super.isContainsModifications() : transactionContext.isContainsModifications();
   }

   @Override
   public void setContainsModifications(boolean b) {
      if (transactionContext == null)
         super.setContainsModifications(b);
      else
         transactionContext.setContainsModifications(b);
   }

   @Override
   public boolean isContainsLocks() {
      return transactionContext == null ? super.isContainsLocks() : transactionContext.isContainsLocks();
   }

   @Override
   public void setContainsLocks(boolean b) {
      if (transactionContext == null)
         super.setContainsLocks(b);
      else
         transactionContext.setContainsLocks(b);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      InvocationContextImpl that = (InvocationContextImpl) o;

      if (globalTransaction != null ? !globalTransaction.equals(that.globalTransaction) : that.globalTransaction != null)
         return false;
      if (transaction != null ? !transaction.equals(that.transaction) : that.transaction != null) return false;
      if (transactionContext != null ? !transactionContext.equals(that.transactionContext) : that.transactionContext != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (transaction != null ? transaction.hashCode() : 0);
      result = 31 * result + (globalTransaction != null ? globalTransaction.hashCode() : 0);
      result = 31 * result + (transactionContext != null ? transactionContext.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "InvocationContextImpl{" +
            "transaction=" + transaction +
            ", globalTransaction=" + globalTransaction +
            ", transactionContext=" + transactionContext +
            ", flags=" + flags +
            ", contextFlags=" + contextFlags +
//            ", invocationLocks=" + locks +
            ", lookedUpEntries size=" + (lookedUpEntries == null ? 0 : lookedUpEntries.size()) +
            '}';
   }
}
