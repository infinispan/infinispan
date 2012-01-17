/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.transaction;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.config.Configuration;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;

/**
 * Base class for both Sync and XAResource enlistment adapters.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public abstract class AbstractEnlistmentAdapter {

   private final int hashCode;

   public AbstractEnlistmentAdapter(CacheTransaction cacheTransaction) {
      hashCode = preComputeHashCode(cacheTransaction);
   }

   public AbstractEnlistmentAdapter() {
      hashCode = 31;
   }

   /**
    * Invoked by TransactionManagers, make sure it's an efficient implementation.
    * System.identityHashCode(x) is NOT an efficient implementation.
    */
   @Override
   public final int hashCode() {
      return this.hashCode;
   }

   private static int preComputeHashCode(final CacheTransaction cacheTx) {
      return 31 + cacheTx.hashCode();
   }
}
