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
package org.infinispan.interceptors.base;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.transaction.LocalTransaction;

/**
 * Acts as a base for all RPC calls
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class BaseRpcInterceptor extends CommandInterceptor {

   protected RpcManager rpcManager;

   protected boolean defaultSynchronous;

   @Inject
   public void inject(RpcManager rpcManager) {
      this.rpcManager = rpcManager;
   }

   @Start
   public void init() {
      defaultSynchronous = cacheConfiguration.clustering().cacheMode().isSynchronous();
   }

   protected final boolean isSynchronous(FlagAffectedCommand command) {
      if (command.hasFlag(Flag.FORCE_SYNCHRONOUS))
         return true;
      else if (command.hasFlag(Flag.FORCE_ASYNCHRONOUS))
         return false;

      return defaultSynchronous;
   }

   protected final boolean isLocalModeForced(FlagAffectedCommand command) {
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (getLog().isTraceEnabled()) getLog().trace("LOCAL mode forced on invocation.  Suppressing clustered events.");
         return true;
      }
      return false;
   }

   protected boolean shouldInvokeRemoteTxCommand(TxInvocationContext ctx) {
      if (!ctx.isOriginLocal()) {
         return false;
      }

      // just testing for empty modifications isn't enough - the Lock API may acquire locks on keys but won't
      // register a Modification.  See ISPN-711.
      LocalTxInvocationContext localCtx = (LocalTxInvocationContext) ctx;
      boolean shouldInvokeRemotely = ctx.hasModifications() || !localCtx.getRemoteLocksAcquired().isEmpty() ||
         localCtx.getCacheTransaction().getTopologyId() != rpcManager.getTopologyId();

      if (getLog().isTraceEnabled()) {
         getLog().tracef("Should invoke remotely? %b. hasModifications=%b, hasRemoteLocksAcquired=%b",
               shouldInvokeRemotely, ctx.hasModifications(), !localCtx.getRemoteLocksAcquired().isEmpty());
      }

      return shouldInvokeRemotely;
   }

   protected static void transactionRemotelyPrepared(TxInvocationContext ctx) {
      if (ctx.isOriginLocal()) {
         ((LocalTransaction)ctx.getCacheTransaction()).markPrepareSent();
      }
   }

   protected final boolean isSyncCommitPhase() {
      return cacheConfiguration.transaction().syncCommitPhase();
   }
}