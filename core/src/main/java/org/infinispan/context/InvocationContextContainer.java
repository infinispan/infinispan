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

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.util.Equivalence;

import javax.transaction.Transaction;

/**
 * Manages the association between an {@link org.infinispan.context.InvocationContext} and the calling thread. Also acts
 * as a factory for creating various types of {@link org.infinispan.context.InvocationContext}s.
 *
 * @author Manik Surtani (manik AT infinispan DOT org)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@SurvivesRestarts
@Scope(Scopes.NAMED_CACHE)
public interface InvocationContextContainer {

   /**
    * To be used when building InvocationContext with {@link #createInvocationContext(boolean, int)} as an indicator
    * of the fact that the size of the keys to be accessed in the context is not known.
    */
   int UNBOUNDED = -1;

   /**
    * Returns the {@link InvocationContext} that is currently associated with the calling thread. Important:
    * implementations of this method are most likely expensive, involving thread locals. It is recommended to cache
    * the result of this method rather than repeating the call.
    *
    * @throws IllegalStateException if there is no context associated with the current thread.
    *
    * @deprecated see implementation for notes
    * @param quiet
    */
   @Deprecated
   InvocationContext getInvocationContext(boolean quiet);


   /**
    * If we are in a tx scope this will return an {@link org.infinispan.context.impl.TxInvocationContext}. Otherwise it
    * will return an {@link org.infinispan.context.impl.NonTxInvocationContext}. Either way, both context will be marked
    * as local, i.e. {@link InvocationContext#isOriginLocal()} will be true.
    */
   InvocationContext createInvocationContext(boolean isWrite, int keyCount);

   /**
    * Creates an invocation context
    *
    * @param tx
    * @return
    */
   InvocationContext createInvocationContext(Transaction tx);

   /**
    * Will create an {@link org.infinispan.context.impl.NonTxInvocationContext} with the {@link
    * org.infinispan.context.impl.NonTxInvocationContext#isOriginLocal()} returning true.
    */
   NonTxInvocationContext createNonTxInvocationContext();

   /**
    * Will create an {@link org.infinispan.context.impl.NonTxInvocationContext} with the {@link
    * org.infinispan.context.impl.NonTxInvocationContext#isOriginLocal()} returning true.
    */
   InvocationContext createSingleKeyNonTxInvocationContext();

   /**
    * Returns a {@link org.infinispan.context.impl.LocalTxInvocationContext}.
    */
   LocalTxInvocationContext createTxInvocationContext();

   /**
    * Returns an {@link org.infinispan.context.impl.RemoteTxInvocationContext}.
    *
    * @param tx remote transaction
    * @param origin the origin of the command, or null if local
    */
   RemoteTxInvocationContext createRemoteTxInvocationContext(RemoteTransaction tx, Address origin);

   /**
    * Returns an {@link org.infinispan.context.impl.NonTxInvocationContext} whose {@link
    * org.infinispan.context.impl.NonTxInvocationContext#isOriginLocal()} flag will be true.
    *
    * @param origin the origin of the command, or null if local
    */
   InvocationContext createRemoteInvocationContext(Address origin);

   /**
    * As {@link #createRemoteInvocationContext(org.infinispan.remoting.transport.Address)},
    * but returning the flags to the context from the Command if any Flag was set.
    *
    * @param cacheCommand
    * @param origin       the origin of the command, or null if local
    */
   InvocationContext createRemoteInvocationContextForCommand(VisitableCommand cacheCommand, Address origin);

   /**
    * Must be called as each thread exists the interceptor chain.
    */
   void clearThreadLocal();
}
