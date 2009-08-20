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
package org.infinispan.interceptors.base;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.rpc.RpcManager;

/**
 * Acts as a base for all RPC calls - subclassed by
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class BaseRpcInterceptor extends CommandInterceptor {

   protected RpcManager rpcManager;

   @Inject
   public void init(RpcManager rpcManager) {
      this.rpcManager = rpcManager;
   }

   protected boolean defaultSynchronous;

   @Start
   public void init() {
      defaultSynchronous = configuration.getCacheMode().isSynchronous();
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);
      if (ctx.isOriginLocal()) {
         rpcManager.broadcastRpcCommand(command, true, false);
      }
      return retVal;
   }


   protected final boolean isSynchronous(InvocationContext ctx) {
      if (ctx.hasFlag(Flag.FORCE_SYNCHRONOUS))
         return true;
      else if (ctx.hasFlag(Flag.FORCE_ASYNCHRONOUS))
         return false;

      return defaultSynchronous;
   }

   protected final boolean isLocalModeForced(InvocationContext ctx) {
      if (ctx.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (trace) log.trace("LOCAL mode forced on invocation.  Suppressing clustered events.");
         return true;
      }
      return false;
   }
}