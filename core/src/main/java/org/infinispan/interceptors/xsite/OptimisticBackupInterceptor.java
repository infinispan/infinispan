/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.interceptors.xsite;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Handles x-site data backups for optimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class OptimisticBackupInterceptor extends BaseBackupInterceptor {

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //if this is an "empty" tx no point replicating it to other clusters
      if (!shouldInvokeRemoteTxCommand(ctx))
         return super.visitPrepareCommand(ctx, command);

      boolean isTxFromRemoteSite = isTxFromRemoteSite(command.getGlobalTransaction());
      if (isTxFromRemoteSite) {
         return invokeNextInterceptor(ctx, command);
      }

      BackupResponse backupResponse = backupSender.backupPrepare(command);
      Object result = invokeNextInterceptor(ctx, command);
      backupSender.processResponses(backupResponse, command, ctx.getTransaction());
      return result;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!shouldInvokeRemoteTxCommand(ctx))
         return super.visitCommitCommand(ctx, command);

      if (isTxFromRemoteSite(command.getGlobalTransaction())) {
         return invokeNextInterceptor(ctx, command);
      }

      BackupResponse backupResponse = backupSender.backupCommit(command);
      Object result = invokeNextInterceptor(ctx, command);
      backupSender.processResponses(backupResponse, command, ctx.getTransaction());
      return result;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!shouldInvokeRemoteTxCommand(ctx))
         return super.visitRollbackCommand(ctx, command);

      if (isTxFromRemoteSite(command.getGlobalTransaction())) {
         return invokeNextInterceptor(ctx, command);
      }

      BackupResponse backupResponse = backupSender.backupRollback(command);
      Object result = invokeNextInterceptor(ctx, command);
      backupSender.processResponses(backupResponse, command, ctx.getTransaction());
      return result;
   }
}
