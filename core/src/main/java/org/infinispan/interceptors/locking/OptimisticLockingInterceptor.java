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

import org.infinispan.CacheException;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.EntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.hash.MurmurHash3;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Locking interceptor to be used by optimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class OptimisticLockingInterceptor extends AbstractTxLockingInterceptor {

   final LockAquisitionVisitor lockAquisitionVisitor = new LockAquisitionVisitor();

   private final static Comparator<Object> keyComparator = new Comparator<Object>() {

      private final MurmurHash3 hash = new MurmurHash3();

      @Override
      public int compare(Object o1, Object o2) {
         return hash.hash(o1) - hash.hash(o2);
      }
   };

   EntryFactory entryFactory;

   @Inject
   public void setDependencies(EntryFactory entryFactory) {
      this.entryFactory = entryFactory;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         abortIfRemoteTransactionInvalid(ctx, command);

         if (command.writesToASingleKey()) {
            //optimisation: don't create another LockReorderingVisitor here as it is not needed.
            log.trace("Not using lock reordering as we have a single key.");
            acquireLocksVisitingCommands(ctx, command);
         } else {
            LockReorderingVisitor lre = new LockReorderingVisitor(command.getModifications());
            if (!lre.hasClear) {
               log.tracef("Using lock reordering, order is: %s", lre.orderedKeys);
               acquireAllLocks(ctx, lre.orderedKeys.iterator());
               ctx.addAllAffectedKeys(lre.orderedKeys);
            } else {
               log.trace("Not using lock reordering as the prepare contains a clear command.");
               acquireLocksVisitingCommands(ctx, command);
            }
         }

         return invokeNextAndCommitIf1Pc(ctx, command);
      } catch (Throwable te) {
         // don't remove the locks here, the rollback command will clear them
         throw te;
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }
   
   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      try {
         for (Object key : dataContainer.keySet())
            entryFactory.wrapEntryForClear(ctx, key);
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      throw new CacheException("Explicit locking is not allowed with optimistic caches!");
   }

   private final class LockAquisitionVisitor extends AbstractVisitor {

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         for (Object key : dataContainer.keySet()) {
            lockAndRegisterBackupLock(txC, key);
            txC.addAffectedKey(key);
         }
         return null;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         for (Object key : command.getMap().keySet()) {
            lockAndRegisterBackupLock(txC, key);
            txC.addAffectedKey(key);
         }
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         lockAndRegisterBackupLock(txC, command.getKey());
         txC.addAffectedKey(command.getKey());
         return null;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         lockAndRegisterBackupLock(txC, command.getKey());
         txC.addAffectedKey(command.getKey());
         return null;
      }
      
      @Override
      public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         if (cdl.localNodeIsOwner(command.getKey())) {
            Object[] compositeKeys = command.getCompositeKeys();
            for (Object key : compositeKeys) {
               lockAndRegisterBackupLock((TxInvocationContext) ctx, key);
            }            
         }
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         lockAndRegisterBackupLock(txC, command.getKey());
         txC.addAffectedKey(command.getKey());
         return null;
      }
   }

   /**
    * This visitor doesn't handle all the possible {@link WriteCommand}s, but only the ones that can be aggregated
    * within the {@link PrepareCommand}.
    */
   private final class LockReorderingVisitor extends AbstractVisitor {

      private boolean hasClear;

      private final SortedSet<Object> orderedKeys = new TreeSet<Object>(keyComparator);

      public LockReorderingVisitor(WriteCommand[] modifications) throws Throwable {
         for (WriteCommand wc : modifications) {
            wc.acceptVisitor(null, this);
         }
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return orderedKeys.add(command.getKey());
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         return orderedKeys.addAll(command.getAffectedKeys());
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         return orderedKeys.add(command.getKey());
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         return orderedKeys.add(command.getKey());
      }

      @Override
      public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         if (cdl.localNodeIsOwner(command.getKey())) {
            Object[] compositeKeys = command.getCompositeKeys();
            orderedKeys.addAll(Arrays.asList(compositeKeys));
         }
         return null;
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         hasClear = true;
         return null;
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         throw new IllegalStateException("Visitable command which require lock acquisition is ignored! " + command);
      }
   }

   private void acquireAllLocks(TxInvocationContext ctx, Iterator<Object> orderedKeys) throws InterruptedException {
      while (orderedKeys.hasNext()) {
         lockAndRegisterBackupLock(ctx, orderedKeys.next());
      }
   }

   private void acquireLocksVisitingCommands(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      for (WriteCommand wc : command.getModifications()) {
         wc.acceptVisitor(ctx, lockAquisitionVisitor);
      }
   }
}
