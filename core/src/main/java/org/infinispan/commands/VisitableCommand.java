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
package org.infinispan.commands;

import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;


/**
 * A type of command that can accept {@link Visitor}s, such as {@link org.infinispan.interceptors.base.CommandInterceptor}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public interface VisitableCommand extends ReplicableCommand {
   /**
    * Accept a visitor, and return the result of accepting this visitor.
    *
    * @param ctx     invocation context
    * @param visitor visitor to accept
    * @return arbitrary return value
    * @throws Throwable in the event of problems
    */
   Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable;

   /**
    * Used by the InboundInvocationHandler to determine whether the command should be invoked or not.
    * @return true if the command should be invoked, false otherwise.
    */
   boolean shouldInvoke(InvocationContext ctx);

   /**
    * Similar to {@link #shouldInvoke(InvocationContext)} but evaluated by {@link InvocationContextInterceptor}.
    * Commands can opt to be discarded in case the cache status is not suited (as {@link InvalidateCommand})
    * @return true if the command should NOT be invoked.
    */
   boolean ignoreCommandOnStatus(ComponentStatus status);

}
