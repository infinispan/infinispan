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
package org.infinispan.transaction.xa.recovery;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.util.Equivalence;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Status;
import java.util.Collection;

/**
 * Extends {@link org.infinispan.transaction.RemoteTransaction} and adds recovery related information and functionality.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareRemoteTransaction extends RemoteTransaction implements RecoveryAwareTransaction {

   private static final Log log = LogFactory.getLog(RecoveryAwareRemoteTransaction.class);

   private boolean prepared;

   private boolean isOrphan;

   private Integer status;

   public RecoveryAwareRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx, int topologyId, Equivalence<Object> keyEquivalence) {
      super(modifications, tx, topologyId, keyEquivalence);
   }

   public RecoveryAwareRemoteTransaction(GlobalTransaction tx, int topologyId, Equivalence<Object> keyEquivalence) {
      super(tx, topologyId, keyEquivalence);
   }

   /**
    * A transaction is in doubt if it is prepared and and it is orphan.
    */
   public boolean isInDoubt() {
      return isPrepared() && isOrphan();
   }

   /**
    * A remote transaction is orphan if the node on which the transaction originated (ie the originator) is no longer
    * part of the cluster.
    */
   public boolean isOrphan() {
      return isOrphan;
   }

   /**
    * Check's if this transaction's originator is no longer part of the cluster (orphan transaction) and updates
    * {@link #isOrphan()}.
    * @param currentMembers The current members of the cache.
    */
   public void computeOrphan(Collection<Address> currentMembers) {
      if (!currentMembers.contains(getGlobalTransaction().getAddress())) {
         if (log.isTraceEnabled()) log.tracef("This transaction's originator has left the cluster: %s", getGlobalTransaction());
         isOrphan = true;
      }
   }

   @Override
   public boolean isPrepared() {
      return prepared;
   }

   @Override
   public void setPrepared(boolean prepared) {
      this.prepared = prepared;
      if (prepared) status = Status.STATUS_PREPARED;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{prepared=" + prepared +
            ", isOrphan=" + isOrphan +
            ", modifications=" + modifications +
            ", lookedUpEntries=" + lookedUpEntries +
            ", tx=" + tx +
            "} ";
   }

   /**
    * Called when after the 2nd phase of a 2PC is successful.
    *
    * @param committed true if tx successfully committed, false if tx successfully rolled back.
    */
   public void markCompleted(boolean committed) {
      status = committed ? Status.STATUS_COMMITTED : Status.STATUS_ROLLEDBACK;
   }

   /**
    * Following values might be returned:
    * <ul>
    *    <li> - {@link Status#STATUS_PREPARED} if the tx is prepared </li>
    *    <li> - {@link Status#STATUS_COMMITTED} if the tx is committed</li>
    *    <li> - {@link Status#STATUS_ROLLEDBACK} if the tx is rollback</li>
    *    <li> - null otherwise</li>
    * </ul>
    */
   public Integer getStatus() {
      return status;
   }
}
