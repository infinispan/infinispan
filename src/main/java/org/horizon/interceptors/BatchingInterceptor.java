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
package org.horizon.interceptors;

import org.horizon.batch.BatchContainer;
import org.horizon.commands.VisitableCommand;
import org.horizon.context.InvocationContext;
import org.horizon.factories.annotations.Inject;
import org.horizon.interceptors.base.CommandInterceptor;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 * Interceptor that captures batched calls and attaches contexts.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 1.0
 */
public class BatchingInterceptor extends CommandInterceptor {
   BatchContainer batchContainer;
   TransactionManager transactionManager;

   @Inject
   private void inject(BatchContainer batchContainer, TransactionManager transactionManager) {
      this.batchContainer = batchContainer;
      this.transactionManager = transactionManager;
   }

   /**
    * Simply check if there is an ongoing tx. <ul> <li>If there is one, this is a no-op and just passes the call up the
    * chain.</li> <li>If there isn't one and there is a batch in progress, resume the batch's tx, pass up, and finally
    * suspend the batch's tx.</li> <li>If there is no batch in progress, just pass the call up the chain.</li> </ul>
    */
   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      Transaction tx = null;
      try {
         // if in a batch, attach tx
         if (transactionManager.getTransaction() == null &&
               (tx = batchContainer.getBatchTransaction()) != null) {
            transactionManager.resume(tx);
         }
         return super.handleDefault(ctx, command);
      }
      finally {
         if (tx != null && transactionManager.getTransaction() != null && batchContainer.isSuspendTxAfterInvocation())
            transactionManager.suspend();
      }
   }
}
