/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Synchronizes the incoming totally ordered transactions with the state transfer.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderStateTransferInterceptor extends CommandInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderStateTransferInterceptor.class);
   private StateTransferManager stateTransferManager;

   @Inject
   public void inject(StateTransferManager stateTransferManager) {
      this.stateTransferManager = stateTransferManager;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return localPrepare(ctx, command);
      }
      return remotePrepare(ctx, command);
   }

   private Object remotePrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      final int topologyId = cacheTopology.getTopologyId();
      ((RemoteTransaction) ctx.getCacheTransaction()).setMissingLookedUpEntries(false);

      if (log.isTraceEnabled()) {
         log.tracef("Remote transaction received %s. Tx topology id is %s and current topology is is %s",
                    ctx.getGlobalTransaction().globalId(), command.getTopologyId(), topologyId);
      }

      if (command.getTopologyId() < topologyId) {
         if (log.isDebugEnabled()) {
            log.debugf("Transaction %s delivered in new topology Id. Discard it because it should be retransmitted",
                       ctx.getGlobalTransaction().globalId());
         }
         throw new RetryPrepareException();
      } else if (command.getTopologyId() > topologyId) {
         throw new IllegalStateException("This should never happen");
      }

      return invokeNextInterceptor(ctx, command);
   }

   private Object localPrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      boolean needsToPrepare = true;
      Object retVal = null;
      while (needsToPrepare) {
         try {
            CacheTopology cacheTopology = stateTransferManager.getCacheTopology();

            command.setTopologyId(cacheTopology.getTopologyId());

            if (log.isTraceEnabled()) {
               log.tracef("Local transaction received %s. setting topology Id to %s",
                          command.getGlobalTransaction().globalId(), command.getTopologyId());
            }

            retVal = invokeNextInterceptor(ctx, command);
            needsToPrepare = false;
         } catch (Throwable throwable) {
            //if we receive a RetryPrepareException it was because the prepare was delivered during a state transfer.
            //Remember that the REBALANCE_START and CH_UPDATE are totally ordered with the prepares and the prepares are
            //unblocked after the rebalance has finished.
            needsToPrepare = needsToRePrepare(throwable);
            if (log.isDebugEnabled()) {
               log.tracef("Exception caught while preparing transaction %s (cause = %s). Needs to retransmit? %s",
                          command.getGlobalTransaction().globalId(), throwable.getCause(), needsToPrepare);
            }

            if (!needsToPrepare) {
               throw throwable;
            }
         }
      }
      return retVal;
   }

   private boolean needsToRePrepare(Throwable throwable) {
      return throwable instanceof RemoteException && throwable.getCause() instanceof RetryPrepareException;
   }
}
