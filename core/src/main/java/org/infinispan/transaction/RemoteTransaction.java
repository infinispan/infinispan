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
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Defines the state of a remotely originated transaction.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class RemoteTransaction extends AbstractCacheTransaction implements Cloneable {

   private static final Log log = LogFactory.getLog(RemoteTransaction.class);

   private volatile boolean valid = true;

   public RemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx, int viewId) {
      super(tx, viewId);
      this.modifications = modifications == null || modifications.length == 0 ? Collections.<WriteCommand>emptyList() : Arrays.asList(modifications);
      lookedUpEntries = new HashMap<Object, CacheEntry>(this.modifications.size());
   }

   public RemoteTransaction(GlobalTransaction tx, int viewId) {
      super(tx, viewId);
      this.modifications = new LinkedList<WriteCommand>();
      lookedUpEntries = new HashMap<Object, CacheEntry>(2);
   }

   public void invalidate() {
      valid = false;
   }

   public void putLookedUpEntry(Object key, CacheEntry e) {
      if (valid) {
         if (log.isTraceEnabled()) {
            log.tracef("Adding key %s to tx %s", key, getGlobalTransaction());
         }
         lookedUpEntries.put(key, e);
      } else {
         throw new InvalidTransactionException("This remote transaction " + getGlobalTransaction() + " is invalid");
      }
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
   @SuppressWarnings("unchecked")
   public Object clone() {
      try {
         RemoteTransaction dolly = (RemoteTransaction) super.clone();
         dolly.modifications = new ArrayList<WriteCommand>(modifications);
         dolly.lookedUpEntries = new HashMap<Object, CacheEntry>(lookedUpEntries);
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
            ", lockedKeys= " + lockedKeys +
            ", backupKeyLocks " + backupKeyLocks +
            ", tx=" + tx +
            '}';
   }
}
