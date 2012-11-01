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
package org.infinispan.transaction.synchronization;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.transaction.AbstractEnlistmentAdapter;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.xa.XAException;

/**
 * {@link Synchronization} implementation for integrating with the TM.
 * See <a href="https://issues.jboss.org/browse/ISPN-888">ISPN-888</a> for more information on this.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class SynchronizationAdapter extends AbstractEnlistmentAdapter implements Synchronization {

   private static final Log log = LogFactory.getLog(SynchronizationAdapter.class);

   private final LocalTransaction localTransaction;
   private final TransactionCoordinator txCoordinator;

   public SynchronizationAdapter(LocalTransaction localTransaction, TransactionCoordinator txCoordinator,
                                 CommandsFactory commandsFactory, RpcManager rpcManager,
                                 TransactionTable transactionTable, ClusteringDependentLogic clusteringLogic,
                                 Configuration configuration) {
      super(localTransaction, commandsFactory, rpcManager, transactionTable, clusteringLogic, configuration);
      this.localTransaction = localTransaction;
      this.txCoordinator = txCoordinator;
   }

   @Override
   public void beforeCompletion() {
      log.tracef("beforeCompletion called for %s", localTransaction);
      try {
         txCoordinator.prepare(localTransaction);
      } catch (XAException e) {
         throw new CacheException("Could not prepare. ", e);//todo shall we just swallow this exception?
      }
   }

   @Override
   public void afterCompletion(int status) {
      if (log.isTraceEnabled()) {
         log.tracef("afterCompletion(%s) called for %s.", status, localTransaction);
      }
      if (status == Status.STATUS_COMMITTED) {
         try {
            txCoordinator.commit(localTransaction, false);
         } catch (XAException e) {
            throw new CacheException("Could not commit.", e);
         }
         releaseLocksForCompletedTransaction(localTransaction);
      } else if (status == Status.STATUS_ROLLEDBACK) {
         try {
            txCoordinator.rollback(localTransaction);
         } catch (XAException e) {
            throw new CacheException("Could not commit.", e);
         }
      } else {
         throw new IllegalArgumentException("Unknown status: " + status);
      }
   }

   @Override
   public String toString() {
      return "SynchronizationAdapter{" +
            "localTransaction=" + localTransaction +
            "} " + super.toString();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SynchronizationAdapter that = (SynchronizationAdapter) o;

      if (localTransaction != null ? !localTransaction.equals(that.localTransaction) : that.localTransaction != null)
         return false;

      return true;
   }
}
