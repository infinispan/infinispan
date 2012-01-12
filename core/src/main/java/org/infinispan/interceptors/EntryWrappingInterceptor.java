/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.interceptors;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.*;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Interceptor in charge with wrapping entries and add them in caller's context.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class EntryWrappingInterceptor extends CommandInterceptor {

   private EntryFactory entryFactory;
   protected DataContainer dataContainer;
   protected ClusteringDependentLogic cll;
   protected final EntryWrappingVisitor entryWrappingVisitor = new EntryWrappingVisitor();

   private static final Log log = LogFactory.getLog(EntryWrappingInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(EntryFactory entryFactory, DataContainer dataContainer, ClusteringDependentLogic cll) {
      this.entryFactory =  entryFactory;
      this.dataContainer = dataContainer;
      this.cll = cll;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!ctx.isOriginLocal() || command.isReplayEntryWrapping()) {
         for (WriteCommand c : command.getModifications()) {
            c.acceptVisitor(ctx, entryWrappingVisitor);
         }
      }
      Object result = invokeNextInterceptor(ctx, command);
      if (command.isOnePhaseCommit()) {
         commitContextEntries(ctx);
      }
      return result;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         commitContextEntries(ctx);
      }
   }

   @Override
   public final Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      try {
         entryFactory.wrapEntryForReading(ctx, command.getKey());
         return invokeNextInterceptor(ctx, command);
      } finally {
         //needed because entries might be added in L1
         if (!ctx.isInTxScope()) commitContextEntries(ctx);
      }
   }

   @Override
   public final Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      if (command.getKeys() != null) {
         for (Object key : command.getKeys())
            entryFactory.wrapEntryForReplace(ctx, key);
      }
      return invokeNextAndApplyChanges(ctx, command);
   }

   @Override
   public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      for (InternalCacheEntry entry : dataContainer.entrySet())
         entryFactory.wrapEntryForClear(ctx, entry.getKey());
      return invokeNextAndApplyChanges(ctx, command);
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      for (Object key : command.getKeys()) {
         entryFactory.wrapEntryForReplace(ctx, key);
      }
      return invokeNextAndApplyChanges(ctx, command);
   }

   @Override
   public final Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      entryFactory.wrapEntryForPut(ctx, command.getKey(), null, !command.isPutIfAbsent());
      return invokeNextAndApplyChanges(ctx, command);
   }
   
   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {      
      entryFactory.wrapEntryForDelta(ctx, command.getDeltaAwareKey(), command.getDelta());  
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public final Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      entryFactory.wrapEntryForRemove(ctx, command.getKey());
      return invokeNextAndApplyChanges(ctx, command);
   }

   @Override
   public final Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      entryFactory.wrapEntryForReplace(ctx, command.getKey());
      return invokeNextAndApplyChanges(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      for (Object key : command.getMap().keySet()) {
         entryFactory.wrapEntryForPut(ctx, key, null, true);
      }
      return invokeNextAndApplyChanges(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      return visitRemoveCommand(ctx, command);
   }

   protected void commitContextEntries(final InvocationContext ctx) {
      final boolean trace = log.isTraceEnabled();
      boolean skipOwnershipCheck = ctx.hasFlag(Flag.SKIP_OWNERSHIP_CHECK);

      if (ctx instanceof SingleKeyNonTxInvocationContext) {
         CacheEntry entry = ((SingleKeyNonTxInvocationContext)ctx).getCacheEntry();
         commitEntryIfNeeded(ctx, skipOwnershipCheck, entry);
      } else {
         Set<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
         Iterator<Map.Entry<Object, CacheEntry>> it = entries.iterator();
         final Log log = getLog();
         while (it.hasNext()) {
            Map.Entry<Object, CacheEntry> e = it.next();
            CacheEntry entry = e.getValue();
            if (!commitEntryIfNeeded(ctx, skipOwnershipCheck, entry)) {
               if (trace) {
                  if (entry==null)
                     log.tracef("Entry for key %s is null : not calling commitUpdate", e.getKey());
                  else
                     log.tracef("Entry for key %s is not changed(%s): not calling commitUpdate", e.getKey(), entry);
               }
            }
         }
      }
   }

   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, boolean skipOwnershipCheck) {
      cll.commitEntry(entry, null, skipOwnershipCheck);
   }

   private Object invokeNextAndApplyChanges(InvocationContext ctx, VisitableCommand command) throws Throwable {
      final Object result = invokeNextInterceptor(ctx, command);
      if (!ctx.isInTxScope()) commitContextEntries(ctx);
      return result;
   }

   private final class EntryWrappingVisitor extends AbstractVisitor {

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         boolean notWrapped = false;
         for (Object key : dataContainer.keySet()) {
            entryFactory.wrapEntryForClear(ctx, key);
            notWrapped = true;
         }
         if (notWrapped)
            invokeNextInterceptor(ctx, command);
         return null;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         boolean notWrapped = false;
         for (Object key : command.getMap().keySet()) {
            if (cll.localNodeIsOwner(key)) {
               entryFactory.wrapEntryForPut(ctx, key, null, true);
               notWrapped = true;
            }
         }
         if (notWrapped)
            invokeNextInterceptor(ctx, command);
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         if (cll.localNodeIsOwner(command.getKey())) {
            entryFactory.wrapEntryForRemove(ctx, command.getKey());
            invokeNextInterceptor(ctx, command);
         }
         return null;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (cll.localNodeIsOwner(command.getKey())) {
            entryFactory.wrapEntryForPut(ctx, command.getKey(), null, !command.isPutIfAbsent());
            invokeNextInterceptor(ctx, command);
         }
         return null;
      }
      
      @Override
      public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         if (cll.localNodeIsOwner(command.getKey())) {              
            entryFactory.wrapEntryForDelta(ctx, command.getDeltaAwareKey(), command.getDelta());
            invokeNextInterceptor(ctx, command);
         }
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         if (cll.localNodeIsOwner(command.getKey())) {
            entryFactory.wrapEntryForReplace(ctx, command.getKey());
            invokeNextInterceptor(ctx, command);
         }
         return null;
      }
   }

   private boolean commitEntryIfNeeded(InvocationContext ctx, boolean skipOwnershipCheck, CacheEntry entry) {
      if (entry != null && entry.isChanged()) {
         commitContextEntry(entry, ctx, skipOwnershipCheck);
         log.tracef("Committed entry %s", entry);
         return true;
      }
      return false;
   }
}
