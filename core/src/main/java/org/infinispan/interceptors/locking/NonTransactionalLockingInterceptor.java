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

package org.infinispan.interceptors.locking;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;

/**
 * Locking interceptor to be used for non-transactional caches.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class NonTransactionalLockingInterceptor extends AbstractLockingInterceptor {

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         if (cll.localNodeIsOwner(command.getKey())) lockForPut(ctx, command, !command.isPutIfAbsent());
         else wrapForRead(ctx, command.getKey());
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
      finally {
         commit(ctx);
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      try {
         for (Object key : command.getMap().keySet()) {
            if (cll.localNodeIsOwner(key)) lockForPut(ctx, key, true);
            else wrapForRead(ctx, key);
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
      finally {
         commit(ctx);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      try {
         if (cll.localNodeIsOwner(command.getKey())) lockForRemove(ctx, command);
         else wrapForRead(ctx, command.getKey());
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      } finally {
         commit(ctx);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         if (cll.localNodeIsOwner(command.getKey())) lockForReplace(ctx, command);
         else wrapForRead(ctx, command.getKey());
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
      finally {
         commit(ctx);
      }
   }

   private void wrapForRead(InvocationContext ctx, Object key) throws InterruptedException {
      entryFactory.wrapEntryForReading(ctx, key);
   }
}