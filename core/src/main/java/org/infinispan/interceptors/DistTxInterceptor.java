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

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.RehashInProgressException;
import org.infinispan.factories.annotations.Inject;

import java.util.HashMap;
import java.util.Map;

/**
 * A special form of the TxInterceptor that is aware of distribution and consistent hashing, and as such only replays
 * methods during a remote prepare that are targeted to this specific cache instance.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DistTxInterceptor extends TxInterceptor {

   DistributionManager dm;

   @Inject
   public void injectDistributionManager(DistributionManager dm) {
      this.dm = dm;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand cmd) throws Throwable {
      if (!dm.getTransactionLogger().beforeCommand(ctx, cmd)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitPrepareCommand(ctx, cmd);
      } finally {
         dm.getTransactionLogger().afterCommand(ctx, cmd);
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand cmd) throws Throwable {
      if (!dm.getTransactionLogger().beforeCommand(ctx, cmd)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitRollbackCommand(ctx, cmd);
      } finally {
         dm.getTransactionLogger().afterCommand(ctx, cmd);
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand cmd) throws Throwable {
      if (!dm.getTransactionLogger().beforeCommand(ctx, cmd)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitCommitCommand(ctx, cmd);
      } finally {
         dm.getTransactionLogger().afterCommand(ctx, cmd);
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand cmd) throws Throwable {
      if (!dm.getTransactionLogger().beforeCommand(ctx, cmd)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitLockControlCommand(ctx, cmd);
      } finally {
         dm.getTransactionLogger().afterCommand(ctx, cmd);
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (!dm.getTransactionLogger().beforeCommand(ctx, command)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitPutKeyValueCommand(ctx, command);
      } finally {
         dm.getTransactionLogger().afterCommand(ctx, command);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      if (!dm.getTransactionLogger().beforeCommand(ctx, command)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitRemoveCommand(ctx, command);
      } finally {
         dm.getTransactionLogger().afterCommand(ctx, command);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      if (!dm.getTransactionLogger().beforeCommand(ctx, command)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitReplaceCommand(ctx, command);
      } finally {
         dm.getTransactionLogger().afterCommand(ctx, command);
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (!dm.getTransactionLogger().beforeCommand(ctx, command)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitClearCommand(ctx, command);
      } finally {
         dm.getTransactionLogger().afterCommand(ctx, command);
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (!dm.getTransactionLogger().beforeCommand(ctx, command)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitPutMapCommand(ctx, command);
      } finally {
         dm.getTransactionLogger().afterCommand(ctx, command);
      }
   }
}
