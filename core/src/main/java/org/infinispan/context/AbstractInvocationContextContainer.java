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

package org.infinispan.context;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.remoting.transport.Address;

import java.util.Set;

/**
 * Base class for InvocationContextContainer implementations.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public abstract class AbstractInvocationContextContainer implements InvocationContextContainer {

   protected final ThreadLocal<InvocationContext> icTl = new ThreadLocal<InvocationContext>();

   public InvocationContext suspend() {
      InvocationContext invocationContext = icTl.get();
      icTl.remove();
      return invocationContext;
   }

   public void resume(InvocationContext ctxt) {
      if (ctxt != null) icTl.set(ctxt);
   }

   @Override
   public InvocationContext peekInvocationContext() {
      return icTl.get();
   }


   @Override
   public InvocationContext createRemoteInvocationContextForCommand(VisitableCommand cacheCommand, Address origin) {
      InvocationContext context = createRemoteInvocationContext(origin);
      if (cacheCommand != null && cacheCommand instanceof FlagAffectedCommand) {
         FlagAffectedCommand command = (FlagAffectedCommand) cacheCommand;
         Set<Flag> flags = command.getFlags();
         if (flags != null && !flags.isEmpty()) {
            return new InvocationContextFlagsOverride(context, flags);
         }
      }
      return context;
   }
}
