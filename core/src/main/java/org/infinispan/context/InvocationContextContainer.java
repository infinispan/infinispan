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
package org.infinispan.context;

import javax.transaction.Transaction;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

/**
 * Manages the association between an {@link org.infinispan.context.InvocationContext} and the
 * calling thread. Also acts as a factory for creating various types of
 * {@link org.infinispan.context.InvocationContext}s.
 * 
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@SurvivesRestarts
@Scope(Scopes.NAMED_CACHE)
public interface InvocationContextContainer {

   /**
    * If we are in a tx scope this will return an
    * {@link org.infinispan.context.impl.TxInvocationContext}. Otherwise it will return an
    * {@link org.infinispan.context.impl.NonTxInvocationContext}. Either way, both context will be
    * marked as local, i.e. {@link InvocationContext#isOriginLocal()} will be true. The context is
    * also associated with the current thread, so further calls to {@link #getInvocationContext()}
    * will return same instance.
    */
   InvocationContext createInvocationContext();

   /**
    * Creates an invocation context
    *
    * @param tx
    * @return
    */
   InvocationContext createInvocationContext(Transaction tx);

   /**
    * Will create an {@link org.infinispan.context.impl.NonTxInvocationContext} with the
    * {@link org.infinispan.context.impl.NonTxInvocationContext#isOriginLocal()} returning true.
    */
   NonTxInvocationContext createNonTxInvocationContext();

   /**
    * Returns a {@link org.infinispan.context.impl.LocalTxInvocationContext}. The context is also
    * associated with the current thread, so further calls to {@link #getInvocationContext()} will
    * return same instance.
    */
   LocalTxInvocationContext createTxInvocationContext();

   /**
    * Returns an {@link org.infinispan.context.impl.RemoteTxInvocationContext}. The context is also
    * associated with the current thread, so further calls to {@link #getInvocationContext()} will
    * return same instance.
    * 
    * @param origin the origin of the command, or null if local
    */
   RemoteTxInvocationContext createRemoteTxInvocationContext(Address origin);

   /**
    * Returns an {@link org.infinispan.context.impl.NonTxInvocationContext} whose
    * {@link org.infinispan.context.impl.NonTxInvocationContext#isOriginLocal()} flag will be true.
    * The context is also associated with the current thread, so further calls to
    * {@link #getInvocationContext()} will return same instance.
    * 
    * @param origin the origin of the command, or null if local
    */
   InvocationContext createRemoteInvocationContext(Address origin);

   /**
    * As {@link #createRemoteInvocationContext(org.infinispan.remoting.transport.Address)},
    * but returning the flags to the context from the Command if any Flag was set.
    * 
    * @param cacheCommand
    * @param origin the origin of the command, or null if local
    */
   InvocationContext createRemoteInvocationContextForCommand(VisitableCommand cacheCommand, Address origin);

   /**
    * Returns the {@link InvocationContext} that is currently associated with the calling thread.
    * Important: implementations of this method is most likely expensive (ThreadLocal.get), it is
    * recommended to cache the result of this method rather than repeating the call.
    * 
    * @throws IllegalStateException
    *            if there is no context associated with the current thread.
    */
   InvocationContext getInvocationContext();

   /**
    * Disassociates thread's invocation context and returns the existing value.
    */
   InvocationContext suspend();

   /**
    * Associates the supplied {@link InvocationContext} with the calling thread.
    */
   void resume(InvocationContext ic);

   /**
    * Returns the {@link InvocationContext} that is currently associated with
    * the calling thread. <b>Important:<b/> implementations of this method is
    * most likely expensive (ThreadLocal.get), it is recommended to cache the
    * result of this method rather than repeating the call.
    *
    * @return the invocation context associated with the calling thread or
    *         null if none has been associated yet
    */
   InvocationContext peekInvocationContext();
}
