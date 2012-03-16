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

package org.infinispan.context;

import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.RemoteTransaction;

import javax.transaction.Transaction;

/**
 * Invocation Context container to be used for non-transactional caches.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class NonTransactionalInvocationContextContainer extends AbstractInvocationContextContainer {

   @Override
   public InvocationContext createInvocationContext(boolean isWrite, int keyCount) {
      if (keyCount == 1) {
         SingleKeyNonTxInvocationContext result = new SingleKeyNonTxInvocationContext(true);
         ctxHolder.set(result);
         return result;
      } else if (keyCount > 0) {
         NonTxInvocationContext ctx = new NonTxInvocationContext(keyCount, true);
         ctxHolder.set(ctx);
         return ctx;
      }
      return createInvocationContext(null);
   }

   @Override
   public InvocationContext createInvocationContext(Transaction tx) {
      return createNonTxInvocationContext();
   }

   @Override
   public NonTxInvocationContext createNonTxInvocationContext() {
      NonTxInvocationContext ctx = new NonTxInvocationContext();
      ctx.setOriginLocal(true);
      ctxHolder.set(ctx);
      return ctx;
   }

   @Override
   public InvocationContext createSingleKeyNonTxInvocationContext() {
      SingleKeyNonTxInvocationContext result = new SingleKeyNonTxInvocationContext(true);
      ctxHolder.set(result);
      return result;
   }

   @Override
   public NonTxInvocationContext createRemoteInvocationContext(Address origin) {
      NonTxInvocationContext ctx = new NonTxInvocationContext();
      ctx.setOrigin(origin);
      ctxHolder.set(ctx);
      return ctx;
   }

   public InvocationContext getInvocationContext() {
      InvocationContext invocationContext = ctxHolder.get();
      if (invocationContext == null)
         throw new IllegalStateException("This method can only be called after associating the current thread with a context");
      return invocationContext;
   }

   @Override
   public LocalTxInvocationContext createTxInvocationContext() {
      throw exception();
   }

   @Override
   public RemoteTxInvocationContext createRemoteTxInvocationContext(
         RemoteTransaction tx, Address origin) {
      throw exception();
   }

   private IllegalStateException exception() {
      return new IllegalStateException("This is a non-transactional cache - why need to build a transactional context for it!");
   }
}
