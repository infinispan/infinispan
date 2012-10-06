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
import org.infinispan.transaction.AbstractCacheTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.transaction.Transaction;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Context to be used for transaction that originated remotely.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public class RemoteTxInvocationContext extends AbstractTxInvocationContext {


   private RemoteTransaction remoteTransaction;

   public RemoteTxInvocationContext() {
   }

   @Override
   public Transaction getTransaction() {
      // this method is only valid for locally originated transactions!
      return null;
   }

   @Override
   public boolean isTransactionValid() {
      // this is always true since we are governed by the originator's transaction
      return true;
   }

   @Override
   public Object getLockOwner() {
      return remoteTransaction.getGlobalTransaction();
   }

   @Override
   public GlobalTransaction getGlobalTransaction() {
      return remoteTransaction.getGlobalTransaction();
   }

   @Override
   public boolean isOriginLocal() {
      return false;
   }

   @Override
   public List<WriteCommand> getModifications() {
      return remoteTransaction.getModifications();
   }

   public void setRemoteTransaction(RemoteTransaction remoteTransaction) {
      this.remoteTransaction = remoteTransaction;
   }

   @Override
   public CacheEntry lookupEntry(Object key) {
      return remoteTransaction.lookupEntry(key);
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return remoteTransaction.getLookedUpEntries();
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      remoteTransaction.putLookedUpEntry(key, e);
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      remoteTransaction.removeLookedUpEntry(key);
   }

   @Override
   public void clearLookedUpEntries() {
      remoteTransaction.clearLookedUpEntries();
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      remoteTransaction.putLookedUpEntries(lookedUpEntries);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof RemoteTxInvocationContext)) return false;
      RemoteTxInvocationContext that = (RemoteTxInvocationContext) o;
      return remoteTransaction.equals(that.remoteTransaction);
   }

   @Override
   public int hashCode() {
      return remoteTransaction.hashCode();
   }

   @Override
   public RemoteTxInvocationContext clone() {
      RemoteTxInvocationContext dolly = (RemoteTxInvocationContext) super.clone();
      dolly.remoteTransaction = (RemoteTransaction) remoteTransaction.clone();
      return dolly;
   }

   @Override
   public AbstractCacheTransaction getCacheTransaction() {
      return remoteTransaction;
   }

   @Override
   public Set<Object> getLockedKeys() {
      return remoteTransaction.getLockedKeys();
   }

   @Override
   public void addLockedKey(Object key) {
      remoteTransaction.registerLockedKey(key);
   }

   @Override
   public void addPendingLockedKey(Object key) {
      remoteTransaction.addPendingLockForKey(key);      
   }
}
