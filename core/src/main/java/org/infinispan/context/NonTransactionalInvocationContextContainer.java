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

import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.remoting.transport.Address;

import javax.transaction.Transaction;

/**
 * Invocation Context container to be used for non-transactional caches.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class NonTransactionalInvocationContextContainer extends AbstractInvocationContextContainer {

   public InvocationContext createInvocationContext(boolean isWrite) {
      return createInvocationContext(null);
   }

   @Override
   public InvocationContext createInvocationContext(Transaction tx) {
      NonTxInvocationContext existing = (NonTxInvocationContext) icTl.get();
      NonTxInvocationContext nonTxContext;
      if (existing == null) {
         nonTxContext = new NonTxInvocationContext();
         icTl.set(nonTxContext);
      } else {
         nonTxContext = existing;
      }
      nonTxContext.setOriginLocal(true);
      return nonTxContext;
   }

   public NonTxInvocationContext createNonTxInvocationContext() {
      InvocationContext existing = icTl.get();
      if (existing != null) {
         NonTxInvocationContext context = (NonTxInvocationContext) existing;
         context.setOriginLocal(true);
         return context;
      }
      NonTxInvocationContext remoteTxContext = new NonTxInvocationContext();
      icTl.set(remoteTxContext);
      return remoteTxContext;
   }

   public NonTxInvocationContext createRemoteInvocationContext(Address origin) {
      NonTxInvocationContext existing = (NonTxInvocationContext) icTl.get();
      if (existing != null) {
         existing.setOriginLocal(false);
         existing.setOrigin(origin);
         return existing;
      }
      NonTxInvocationContext remoteNonTxContext = new NonTxInvocationContext();
      remoteNonTxContext.setOriginLocal(false);
      remoteNonTxContext.setOrigin(origin);
      icTl.set(remoteNonTxContext);
      return remoteNonTxContext;
   }

   public InvocationContext getInvocationContext() {
      InvocationContext invocationContext = icTl.get();
      if (invocationContext == null)
         throw new IllegalStateException("This method can only be called after associating the current thread with a context");
      return invocationContext;
   }

   private IllegalStateException exception() {
      return new IllegalStateException("This is a non-transactional cache - why need to build a transactional context for it!");
   }

   public LocalTxInvocationContext createTxInvocationContext() {
      throw exception();
   }

   public RemoteTxInvocationContext createRemoteTxInvocationContext(Address origin) {
      throw exception();
   }
}
