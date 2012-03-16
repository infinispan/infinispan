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
package org.infinispan.commands.remote;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Base class for RPC commands.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class BaseRpcInvokingCommand extends BaseRpcCommand {

   protected InterceptorChain interceptorChain;
   protected InvocationContextContainer icc;

   private static final Log log = LogFactory.getLog(BaseRpcInvokingCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   protected BaseRpcInvokingCommand(String cacheName) {
      super(cacheName);
   }

   public void init(InterceptorChain interceptorChain, InvocationContextContainer icc) {
      this.interceptorChain = interceptorChain;
      this.icc = icc;
   }

   protected final Object processVisitableCommand(ReplicableCommand cacheCommand) throws Throwable {
      if (cacheCommand instanceof VisitableCommand) {
         VisitableCommand vc = (VisitableCommand) cacheCommand;
         final InvocationContext ctx = icc.createRemoteInvocationContextForCommand(vc, getOrigin());
         if (vc.shouldInvoke(ctx)) {
            if (trace) log.tracef("Invoking command %s, with originLocal flag set to %b", cacheCommand, ctx.isOriginLocal());
            return interceptorChain.invoke(ctx, vc);
         } else {
            if (trace) log.tracef("Not invoking command %s since shouldInvoke() returned false with context %s", cacheCommand, ctx);
            return null;
         }
         // we only need to return values for a set of remote calls; not every call.
      } else {
         throw new RuntimeException("Do we still need to deal with non-visitable commands? (" + cacheCommand.getClass().getName() + ")");
      }
   }
}
