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
package org.infinispan.interceptors;

import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 * Interceptor that captures batched calls and attaches contexts.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class BatchingInterceptor extends CommandInterceptor {
   BatchContainer batchContainer;
   TransactionManager transactionManager;
   InvocationContextContainer icc;

   private static final Log log = LogFactory.getLog(BatchingInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   private void inject(BatchContainer batchContainer, TransactionManager transactionManager, InvocationContextContainer icc) {
      this.batchContainer = batchContainer;
      this.transactionManager = transactionManager;
      this.icc = icc;
   }

   /**
    * Simply check if there is an ongoing tx. <ul> <li>If there is one, this is a no-op and just passes the call up the
    * chain.</li> <li>If there isn't one and there is a batch in progress, resume the batch's tx, pass up, and finally
    * suspend the batch's tx.</li> <li>If there is no batch in progress, just pass the call up the chain.</li> </ul>
    */
   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      Transaction tx;
      if (!ctx.isOriginLocal()) return invokeNextInterceptor(ctx, command);
      // if in a batch, attach tx
      if (transactionManager.getTransaction() == null && (tx = batchContainer.getBatchTransaction()) != null) {
         try {
            transactionManager.resume(tx);
            //If there's no ongoing tx then BatchingInterceptor creates one and then invokes next interceptor,
            // so that all interceptors in the stack will be executed in a transactional context.
            // This is where a new context (TxInvocationContext) is created, as the existing context is not transactional: NonTxInvocationContext.
            InvocationContext txContext = icc.createInvocationContext(true, -1);
            txContext.setFlags(ctx.getFlags());
            return invokeNextInterceptor(txContext, command);
         } finally {
            if (transactionManager.getTransaction() != null && batchContainer.isSuspendTxAfterInvocation())
               transactionManager.suspend();
         }
      } else {
         return invokeNextInterceptor(ctx, command);
      }
   }
}
