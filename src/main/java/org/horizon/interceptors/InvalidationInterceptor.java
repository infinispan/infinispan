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

import org.horizon.commands.AbstractVisitor;
import org.horizon.commands.CommandsFactory;
import org.horizon.commands.VisitableCommand;
import org.horizon.commands.tx.PrepareCommand;
import org.horizon.commands.write.ClearCommand;
import org.horizon.commands.write.DataWriteCommand;
import org.horizon.commands.write.InvalidateCommand;
import org.horizon.commands.write.PutKeyValueCommand;
import org.horizon.commands.write.PutMapCommand;
import org.horizon.commands.write.RemoveCommand;
import org.horizon.commands.write.ReplaceCommand;
import org.horizon.commands.write.WriteCommand;
import org.horizon.context.InvocationContext;
import org.horizon.context.TransactionContext;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.Start;
import org.horizon.interceptors.base.BaseRpcInterceptor;
import org.horizon.jmx.annotations.ManagedAttribute;
import org.horizon.jmx.annotations.ManagedOperation;
import org.horizon.transaction.GlobalTransaction;
import org.horizon.transaction.TransactionTable;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This interceptor acts as a replacement to the replication interceptor when the CacheImpl is configured with
 * ClusteredSyncMode as INVALIDATE.
 * <p/>
 * The idea is that rather than replicating changes to all caches in a cluster when write methods are called, simply
 * broadcast an {@link InvalidateCommand} on the remote caches containing all keys modified.  This allows the remote
 * cache to look up the value in a shared cache loader which would have been updated with the changes.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 1.0
 */
public class InvalidationInterceptor extends BaseRpcInterceptor {
   private final AtomicLong invalidations = new AtomicLong(0);
   protected Map<GlobalTransaction, List<VisitableCommand>> txMods;
   private CommandsFactory commandsFactory;
   private boolean statsEnabled;

   @Inject
   public void injectDependencies(CommandsFactory commandsFactory) {
      this.commandsFactory = commandsFactory;
   }

   @Start
   private void initTxMap() {
      this.setStatisticsEnabled(configuration.isExposeManagementStatistics());
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleInvalidate(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleInvalidate(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleInvalidate(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      // just broadcast the clear command - this is simplest!
      Object retval = invokeNextInterceptor(ctx, command);
      if (ctx.isOriginLocal()) replicateCall(ctx, command, defaultSynchronous);
      return retval;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object[] keys = command.getMap() == null ? null : command.getMap().keySet().toArray();
      return handleInvalidate(ctx, command, keys);
   }

   @Override
   public Object visitPrepareCommand(InvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      Transaction tx = ctx.getTransaction();
      if (tx != null) {
         if (trace) log.trace("Entering InvalidationInterceptor's prepare phase");
         // fetch the modifications before the transaction is committed (and thus removed from the txTable)
         GlobalTransaction gtx = ctx.getGlobalTransaction();
         TransactionContext transactionContext = ctx.getTransactionContext();
         if (transactionContext == null)
            throw new IllegalStateException("cannot find transaction transactionContext for " + gtx);

         if (transactionContext.hasModifications()) {
            List<WriteCommand> mods;
            if (transactionContext.hasLocalModifications()) {
               mods = Arrays.asList(command.getModifications());
               mods.removeAll(transactionContext.getLocalModifications());
            } else {
               mods = Arrays.asList(command.getModifications());
            }
            broadcastInvalidate(mods, tx, ctx);
         } else {
            if (trace) log.trace("Nothing to invalidate - no modifications in the transaction.");
         }
      }
      return retval;
   }

   private Object handleInvalidate(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      return handleInvalidate(ctx, command, command.getKey());
   }

   private Object handleInvalidate(InvocationContext ctx, WriteCommand command, Object... keys) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      if (command.isSuccessful()) {
         Transaction tx = ctx.getTransaction();
         if (log.isDebugEnabled()) log.debug("Is a CRUD method");
         if (keys != null && keys.length != 0) {
            // could be potentially TRANSACTIONAL.  Ignore if it is, until we see a prepare().
            if (tx == null || !TransactionTable.isValid(tx)) {
               // the no-tx case:
               //replicate an evict call.
               invalidateAcrossCluster(isSynchronous(ctx), ctx, keys);
            } else {
               if (isLocalModeForced(ctx)) ctx.getTransactionContext().addLocalModification(command);
            }
         }
      }
      return retval;
   }

   private void broadcastInvalidate(List<WriteCommand> modifications, Transaction tx, InvocationContext ctx) throws Throwable {
      if (ctx.getTransaction() != null && !isLocalModeForced(ctx)) {
         if (modifications == null || modifications.isEmpty()) return;
         InvalidationFilterVisitor filterVisitor = new InvalidationFilterVisitor(modifications.size());
         filterVisitor.visitCollection(null, modifications);

         if (filterVisitor.containsPutForExternalRead) {
            log.debug("Modification list contains a putForExternalRead operation.  Not invalidating.");
         } else {
            try {
               invalidateAcrossCluster(defaultSynchronous, ctx, filterVisitor.result.toArray());
            }
            catch (Throwable t) {
               log.warn("Unable to broadcast evicts as a part of the prepare phase.  Rolling back.", t);
               try {
                  tx.setRollbackOnly();
               }
               catch (SystemException se) {
                  throw new RuntimeException("setting tx rollback failed ", se);
               }
               if (t instanceof RuntimeException)
                  throw (RuntimeException) t;
               else
                  throw new RuntimeException("Unable to broadcast invalidation messages", t);
            }
         }
      }
   }

   public static class InvalidationFilterVisitor extends AbstractVisitor {
      Set<Object> result;
      public boolean containsPutForExternalRead;

      public InvalidationFilterVisitor(int maxSetSize) {
         result = new HashSet<Object>(maxSetSize);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         result.add(command.getKey());
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         result.add(command.getKey());
         return null;
      }
   }


   protected void invalidateAcrossCluster(boolean synchronous, InvocationContext ctx, Object[] keys) throws Throwable {
      if (!isLocalModeForced(ctx)) {
         // increment invalidations counter if statistics maintained
         incrementInvalidations();
         InvalidateCommand command = commandsFactory.buildInvalidateCommand(keys);
         if (log.isDebugEnabled())
            log.debug("Cache [" + rpcManager.getTransport().getAddress() + "] replicating " + command);
         // voila, invalidated!
         replicateCall(ctx, command, synchronous);
      }
   }

   private void incrementInvalidations() {
      if (statsEnabled) invalidations.incrementAndGet();
   }

   @ManagedOperation
   public void resetStatistics() {
      invalidations.set(0);
   }

   @ManagedOperation
   public Map<String, Object> dumpStatistics() {
      Map<String, Object> retval = new HashMap<String, Object>();
      retval.put("Invalidations", invalidations);
      return retval;
   }

   @ManagedAttribute
   public boolean getStatisticsEnabled() {
      return this.statsEnabled;
   }

   @ManagedAttribute
   public void setStatisticsEnabled(boolean enabled) {
      this.statsEnabled = enabled;
   }

   @ManagedAttribute(description = "number of invalidations")
   public long getInvalidations() {
      return invalidations.get();
   }
}
