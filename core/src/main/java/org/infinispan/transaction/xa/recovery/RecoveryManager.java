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

import org.infinispan.remoting.transport.Address;

import javax.transaction.xa.Xid;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * RecoveryManager is the component responsible with managing recovery related information and the functionality
 * associated with it. Refer to <a href="http://community.jboss.org/wiki/Transactionrecoverydesign">this</a> document
 * for details on the design of recovery.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public interface RecoveryManager {

   /**
    * Returns the list of transactions in prepared state both from local and remote cluster nodes.
    * Implementation can take advantage of several optimisations:
    * <pre>
    * - in order to get all tx from the cluster a broadcast is performed. This can be performed only once (assuming the call
    *   is successful), the first time this method is called. After that a local, cached list of tx prepared on this node is returned.
    * - during the broadcast just return the list of prepared transactions that are not originated on other active nodes of the
    * cluster.
    * </pre>
    */
   RecoveryIterator getPreparedTransactionsFromCluster();


   /**
    * Removes from the specified nodes (or all nodes if value of param 'where'  is null) the recovery information associated with
    * these Xids.
    * @param where on which nodes should this be executed.
    * @param xid the list of xids to be removed.
    * @param sync weather or not this call should be executed sync
    */
   void removeRecoveryInformation(Collection<Address> where, Xid xid, boolean sync);

   /**
    * Remove recovery information stored on this node (doesn't involve any rpc).
    *
    * @param xids which recovery info to remove
    * @see  #removeRecoveryInformation(java.util.Collection, javax.transaction.xa.Xid, boolean)
    */
   void removeLocalRecoveryInformation(List<Xid> xids);

   /**
    * Returns the list of remote transaction prepared on this node for which the originator(i.e. the node where the tx
    * stared) is no longer part of the cluster.
    *
    * @see org.infinispan.transaction.RemoteTransaction#isInDoubt()
    */
   List<Xid> getLocalInDoubtTransactions();


   /**
   * Stateful structure allowing prepared-tx retrieval in a batch-oriented manner,
    * as required by {@link javax.transaction.xa.XAResource#recover(int)}.
   */
   interface RecoveryIterator extends Iterator<Xid[]> {

      public static final Xid[] NOTHING = new Xid[]{};

      /**
       * Exhaust the iterator. After this call, {@link #hasNext()} returns false.
       */
      Xid[] all();
   }
}
