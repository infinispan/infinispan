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
package org.infinispan.interceptors;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.modifications.Clear;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writes modifications back to the store on the way out: stores modifications back through the CacheLoader, either
 * after each method call (no TXs), or at TX commit.
 *
 * @author Bela Ban
 * @since 4.0
 */
@MBean(objectName = "CacheStore", description = "Component that handles storing of entries to a CacheStore from memory.")
public class CacheStoreInterceptor extends JmxStatsCommandInterceptor {
   CacheLoaderManagerConfig loaderConfig = null;
   private Map<GlobalTransaction, Integer> txStores;
   private Map<GlobalTransaction, Set<Object>> preparingTxs;
   final AtomicLong cacheStores = new AtomicLong(0);
   CacheStore store;
   private CacheLoaderManager loaderManager;

   public CacheStoreInterceptor() {
      log = LogFactory.getLog(getClass());
      trace = log.isTraceEnabled();
   }

   @Inject
   protected void init(CacheLoaderManager loaderManager) {
      this.loaderManager = loaderManager;
   }

   @Start(priority = 15)
   protected void start() {
      store = loaderManager.getCacheStore();
      this.setStatisticsEnabled(configuration.isExposeJmxStatistics());
      loaderConfig = configuration.getCacheLoaderManagerConfig();
      txStores = new ConcurrentHashMap<GlobalTransaction, Integer>(64, 0.75f, configuration.getConcurrencyLevel());
      preparingTxs = new ConcurrentHashMap<GlobalTransaction, Set<Object>>(64, 0.75f, configuration.getConcurrencyLevel());
   }

   /**
    * if this is a shared cache loader and the call is of remote origin, pass up the chain
    */
   public final boolean skip(InvocationContext ctx, VisitableCommand command) {
      if (store == null) return true;  // could be because the cache loader oes not implement cache store
      if ((!ctx.isOriginLocal() && loaderConfig.isShared()) || ctx.hasFlag(Flag.SKIP_CACHE_STORE)) {
         if (trace) log.trace("Passing up method call and bypassing this interceptor since the cache loader is shared and this call originated remotely.");
         return true;
      }
      return false;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!skip(ctx, command)) {
         if (ctx.hasModifications()) {
            // this is a commit call.
            GlobalTransaction tx = ctx.getGlobalTransaction();
            if (trace) log.trace("Calling loader.commit() for transaction {0}", tx);
            try {
               store.commit(tx);
            }
            catch (Throwable t) {
               preparingTxs.remove(tx);
               throw t;
            }
            if (getStatisticsEnabled()) {
               Integer puts = txStores.get(tx);
               if (puts != null) {
                  cacheStores.getAndAdd(puts);
               }
               txStores.remove(tx);
            }
            return invokeNextInterceptor(ctx, command);
         } else {
            if (trace) log.trace("Commit called with no modifications; ignoring.");
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!skip(ctx, command)) {
         if (trace) log.trace("transactional so don't put stuff in the cloader yet.");
         if (ctx.hasModifications()) {
            GlobalTransaction tx = ctx.getGlobalTransaction();
            // this is a rollback method
            if (preparingTxs.containsKey(tx)) {
               preparingTxs.remove(tx);
               store.rollback(tx);
            }
            if (getStatisticsEnabled()) txStores.remove(tx);
         } else {
            if (trace) log.trace("Rollback called with no modifications; ignoring.");
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!skip(ctx, command)) {
         if (trace) log.trace("transactional so don't put stuff in the cloader yet.");
         prepareCacheLoader(ctx, command.getGlobalTransaction(), ctx, command.isOnePhaseCommit());
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      if (!skip(ctx, command) && !ctx.isInTxScope() && command.isSuccessful()) {
         Object key = command.getKey();
         boolean resp = store.remove(key);
         if (trace) log.trace("Removed entry under key {0} and got response {1} from CacheStore", key, resp);
      }
      return retval;
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (!skip(ctx, command) && !ctx.isInTxScope()) {
         store.clear();
         if (trace) log.trace("Cleared cache store");
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (skip(ctx, command) || ctx.isInTxScope() || !command.isSuccessful()) return returnValue;

      Object key = command.getKey();
      InternalCacheEntry se = getStoredEntry(key, ctx);
      store.store(se);
      if (trace) log.trace("Stored entry {0} under key {1}", se, key);
      if (getStatisticsEnabled()) cacheStores.incrementAndGet();

      return returnValue;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (skip(ctx, command) || ctx.isInTxScope() || !command.isSuccessful()) return returnValue;

      Object key = command.getKey();
      InternalCacheEntry se = getStoredEntry(key, ctx);
      store.store(se);
      if (trace) log.trace("Stored entry {0} under key {1}", se, key);
      if (getStatisticsEnabled()) cacheStores.incrementAndGet();

      return returnValue;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (skip(ctx, command) || ctx.isInTxScope()) return returnValue;

      Map<Object, Object> map = command.getMap();
      for (Object key : map.keySet()) {
         InternalCacheEntry se = getStoredEntry(key, ctx);
         store.store(se);
         if (trace) log.trace("Stored entry {0} under key {1}", se, key);
      }
      if (getStatisticsEnabled()) cacheStores.getAndAdd(map.size());
      return returnValue;
   }

   private void prepareCacheLoader(TxInvocationContext ctx, GlobalTransaction gtx, TxInvocationContext transactionContext, boolean onePhase) throws Throwable {
      if (transactionContext == null) {
         throw new Exception("transactionContext for transaction " + gtx + " not found in transaction table");
      }
      List<WriteCommand> modifications = transactionContext.getModifications();
      if (modifications.isEmpty()) {
         if (trace) log.trace("Transaction has not logged any modifications!");
         return;
      }
      if (trace) log.trace("Cache loader modification list: {0}", modifications);
      StoreModificationsBuilder modsBuilder = new StoreModificationsBuilder(getStatisticsEnabled());
      for (WriteCommand cacheCommand : modifications) cacheCommand.acceptVisitor(ctx, modsBuilder);
      int numMods = modsBuilder.modifications.size();
      if (trace) log.trace("Converted method calls to cache loader modifications.  List size: {0}", numMods);

      if (numMods > 0) {
         GlobalTransaction tx = transactionContext.getGlobalTransaction();
         store.prepare(modsBuilder.modifications, tx, onePhase);

         preparingTxs.put(tx, modsBuilder.affectedKeys);
         if (getStatisticsEnabled() && modsBuilder.putCount > 0) {
            txStores.put(tx, modsBuilder.putCount);
         }
      }
   }

   public class StoreModificationsBuilder extends AbstractVisitor {

      boolean generateStatistics;

      int putCount;

      Set<Object> affectedKeys = new HashSet<Object>();

      List<Modification> modifications = new ArrayList<Modification>();

      public StoreModificationsBuilder(boolean generateStatistics) {
         this.generateStatistics = generateStatistics;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (generateStatistics) putCount++;
         modifications.add(new Store(getStoredEntry(command.getKey(), ctx)));
         affectedKeys.add(command.getKey());
         return null;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         Map<Object, Object> map = command.getMap();
         if (generateStatistics) putCount += map.size();
         affectedKeys.addAll(map.keySet());
         for (Object key : map.keySet()) modifications.add(new Store(getStoredEntry(key, ctx)));
         return null;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         modifications.add(new Remove(command.getKey()));
         affectedKeys.add(command.getKey());
         return null;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         modifications.add(new Clear());
         return null;
      }
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset statistics")
   public void resetStatistics() {
      cacheStores.set(0);
   }

   @ManagedAttribute(description = "number of cache loader stores")
   @Metric(displayName = "Number of cache stores", measurementType = MeasurementType.TRENDSUP)
   public long getCacheLoaderStores() {
      return cacheStores.get();
   }

   InternalCacheEntry getStoredEntry(Object key, InvocationContext ctx) {
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry instanceof InternalCacheEntry) {
         return (InternalCacheEntry) entry;
      } else {
         return InternalEntryFactory.create(entry.getKey(), entry.getValue(), entry.getLifespan(), entry.getMaxIdle());
      }
   }
}
