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

import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.BackupResponse;

/**
 * Handles x-site data backups for non-transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class NonTransactionalBackupInterceptor extends BaseBackupInterceptor {

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWrite(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWrite(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWrite(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleWrite(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWrite(ctx, command);
   }

   private Object handleWrite(InvocationContext ctx, WriteCommand command) throws Throwable {
      if (!ctx.isOriginLocal() || command.hasFlag(Flag.SKIP_XSITE_BACKUP))
         return invokeNextInterceptor(ctx, command);
      BackupResponse backupResponse = backupSender.backupWrite(command);
      Object result = invokeNextInterceptor(ctx, command);
      backupSender.processResponses(backupResponse, command);
      return result;
   }
}
