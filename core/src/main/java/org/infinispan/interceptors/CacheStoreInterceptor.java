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

import org.infinispan.atomic.AtomicHashMap;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.*;
import org.infinispan.configuration.cache.LoadersConfiguration;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.DeltaAwareCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.modifications.Clear;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.context.Flag.SKIP_CACHE_STORE;
import static org.infinispan.context.Flag.SKIP_SHARED_CACHE_STORE;

/**
 * Writes modifications back to the store on the way out: stores modifications back through the CacheLoader, either
 * after each method call (no TXs), or at TX commit.
 *
 * @author Bela Ban
 * @since 4.0
 */
@MBean(objectName = "CacheStore", description = "Component that handles storing of entries to a CacheStore from memory.")
public class CacheStoreInterceptor extends JmxStatsCommandInterceptor {
   LoadersConfiguration loaderConfig = null;
   private Map<GlobalTransaction, Integer> txStores;
   private Map<GlobalTransaction, Set<Object>> preparingTxs;
   final AtomicLong cacheStores = new AtomicLong(0);
   CacheStore store;
   private CacheLoaderManager loaderManager;
   private InternalEntryFactory entryFactory;
   private TransactionManager transactionManager;
   protected volatile boolean enabled = true;

   private static final Log log = LogFactory.getLog(CacheStoreInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   protected void init(CacheLoaderManager loaderManager, InternalEntryFactory entryFactory, TransactionManager transactionManager) {
      this.loaderManager = loaderManager;
      this.entryFactory = entryFactory;
      this.transactionManager = transactionManager;
   }

   @Start(priority = 15)
   protected void start() {
      store = loaderManager.getCacheStore();
      this.setStatisticsEnabled(cacheConfiguration.jmxStatistics().enabled());
      loaderConfig = cacheConfiguration.loaders();
      int concurrencyLevel = cacheConfiguration.locking().concurrencyLevel();
      txStores = CollectionFactory.makeConcurrentMap(64, concurrencyLevel);
      preparingTxs = CollectionFactory.makeConcurrentMap(64, concurrencyLevel);
   }

   /**
    * if this is a shared cache loader and the call is of remote origin, pass up the chain
    */
   protected boolean skip(InvocationContext ctx, FlagAffectedCommand command) {
      return skip(ctx, command.hasFlag(SKIP_CACHE_STORE), command.hasFlag(SKIP_SHARED_CACHE_STORE));
   }

   protected boolean skip(InvocationContext ctx) {
      return skip(ctx, false, false);
   }

   private boolean skip(InvocationContext ctx, boolean skipCacheStore, boolean skipSharedCacheStore) {
      if (store == null || !enabled) return true;  // could be because the cache loader does not implement cache store, or the store is disabled
      if ((!ctx.isOriginLocal() && loaderConfig.shared()) || skipCacheStore) {
         log.trace("Skipping cache store since the cache loader is shared and we are not the originator.");
         return true;
      }

      if (loaderConfig.shared() && skipSharedCacheStore) {
         log.trace("Explicitly requested to skip storage if cache store is shared - and it is.");
         return true;
      }

      return false;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!skip(ctx))
         commitCommand(ctx);

      return invokeNextInterceptor(ctx, command);
   }

   protected void commitCommand(TxInvocationContext ctx) throws Throwable {
      if (!ctx.getCacheTransaction().getAllModifications().isEmpty()) {
         // this is a commit call.
         GlobalTransaction tx = ctx.getGlobalTransaction();
         if (getLog().isTraceEnabled()) getLog().tracef("Calling loader.commit() for transaction %s", tx);

         //hack for ISPN-586. This should be dropped once a proper fix for ISPN-604 is in place
         Transaction xaTx = null;
         if (transactionManager != null) {
            xaTx = transactionManager.suspend();
         }

         try {
            store.commit(tx);
         } finally {
            // Regardless of outcome, remove from preparing txs
            preparingTxs.remove(tx);

            //part of the hack for ISPN-586
            if (transactionManager != null && xaTx != null) {
               transactionManager.resume(xaTx);
            }
         }
         if (getStatisticsEnabled()) {
            Integer puts = txStores.get(tx);
            if (puts != null) {
               cacheStores.getAndAdd(puts);
            }
            txStores.remove(tx);
         }
      } else {
         if (getLog().isTraceEnabled()) getLog().trace("Commit called with no modifications; ignoring.");
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!skip(ctx)) {
         if (getLog().isTraceEnabled()) getLog().trace("Transactional so don't put stuff in the cache store yet.");
         if (!ctx.getCacheTransaction().getAllModifications().isEmpty()) {
            GlobalTransaction tx = ctx.getGlobalTransaction();
            // this is a rollback method
            if (preparingTxs.containsKey(tx)) {
               preparingTxs.remove(tx);
               store.rollback(tx);
            }
            if (getStatisticsEnabled()) txStores.remove(tx);
         } else {
            if (getLog().isTraceEnabled()) getLog().trace("Rollback called with no modifications; ignoring.");
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!skip(ctx)) {
         if (getLog().isTraceEnabled()) getLog().trace("Transactional so don't put stuff in the cache store yet.");
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
         if (getLog().isTraceEnabled()) getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key, resp);
      }
      return retval;
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (!skip(ctx, command) && !ctx.isInTxScope())
         clearCacheStore();

      return invokeNextInterceptor(ctx, command);
   }

   protected void clearCacheStore() throws CacheLoaderException {
      store.clear();
      if (getLog().isTraceEnabled()) getLog().trace("Cleared cache store");
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (skip(ctx, command) || ctx.isInTxScope() || !command.isSuccessful()) return returnValue;

      Object key = command.getKey();
      InternalCacheEntry se = getStoredEntry(key, ctx);
      store.store(se);
      if (getLog().isTraceEnabled()) getLog().tracef("Stored entry %s under key %s", se, key);
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
      if (getLog().isTraceEnabled()) getLog().tracef("Stored entry %s under key %s", se, key);
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
         if (getLog().isTraceEnabled()) getLog().tracef("Stored entry %s under key %s", se, key);
      }
      if (getStatisticsEnabled()) cacheStores.getAndAdd(map.size());
      return returnValue;
   }

   protected final void prepareCacheLoader(TxInvocationContext ctx, GlobalTransaction gtx, TxInvocationContext transactionContext, boolean onePhase) throws Throwable {
      if (transactionContext == null) {
         throw new Exception("transactionContext for transaction " + gtx + " not found in transaction table");
      }

      List<WriteCommand> modifications = transactionContext.getCacheTransaction().getAllModifications();
      if (modifications.isEmpty()) {
         if (getLog().isTraceEnabled()) getLog().trace("Transaction has not logged any modifications!");
         return;
      }
      if (getLog().isTraceEnabled()) getLog().tracef("Cache loader modification list: %s", modifications);
      StoreModificationsBuilder modsBuilder = new StoreModificationsBuilder(getStatisticsEnabled(), modifications.size());
      for (WriteCommand cacheCommand : modifications) {
         if (!skip(ctx, cacheCommand)) {
            cacheCommand.acceptVisitor(ctx, modsBuilder);
         }
      }
      int numMods = modsBuilder.modifications.size();
      if (getLog().isTraceEnabled()) getLog().tracef("Converted method calls to cache loader modifications.  List size: %s", numMods);

      if (numMods > 0) {
         GlobalTransaction tx = transactionContext.getGlobalTransaction();
         store.prepare(modsBuilder.modifications, tx, onePhase);


         boolean shouldCountStores = getStatisticsEnabled() && modsBuilder.putCount > 0;
         if (!onePhase) {
            preparingTxs.put(tx, modsBuilder.affectedKeys);
            if (shouldCountStores) {
               txStores.put(tx, modsBuilder.putCount);
            }
         } else if (shouldCountStores) {
            cacheStores.getAndAdd(modsBuilder.putCount);
         }
      }
   }

   protected boolean skipKey(Object key) {
      return false;
   }

   public class StoreModificationsBuilder extends AbstractVisitor {

      private final boolean generateStatistics;
      int putCount;
      private final Set<Object> affectedKeys;
      private final List<Modification> modifications;

      public StoreModificationsBuilder(boolean generateStatistics, int numMods) {
         this.generateStatistics = generateStatistics;
         affectedKeys = new HashSet<Object>(numMods);
         modifications = new ArrayList<Modification>(numMods);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return visitSingleStore(ctx, command.getKey());
      }

      @Override
      public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         if (!skipKey(command.getKey())) {
            if (generateStatistics) putCount++;
            CacheEntry entry = ctx.lookupEntry(command.getKey());
            InternalCacheEntry ice;
            if (entry instanceof InternalCacheEntry) {
               ice = (InternalCacheEntry) entry;
            } else if (entry instanceof DeltaAwareCacheEntry) {
               AtomicHashMap<?,?> uncommittedChanges = ((DeltaAwareCacheEntry) entry).getUncommittedChages();
               ice = entryFactory.create(entry.getKey(), uncommittedChanges, entry.getMetadata(), entry.getLifespan(), entry.getMaxIdle());
            } else {
               ice = entryFactory.create(entry);
            }

            modifications.add(new Store(ice));
            affectedKeys.add(command.getKey());
         }
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         return visitSingleStore(ctx, command.getKey());
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         Map<Object, Object> map = command.getMap();
         for (Object key : map.keySet())
            visitSingleStore(ctx, key);
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         Object key = command.getKey();
         if (!skipKey(key)) {
            modifications.add(new Remove(key));
            affectedKeys.add(command.getKey());
         }
         return null;
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         modifications.add(new Clear());
         return null;
      }

      private Object visitSingleStore(InvocationContext ctx, Object key) throws Throwable {
         if (!skipKey(key)) {
            if (generateStatistics) putCount++;
            modifications.add(new Store(getStoredEntry(key, ctx)));
            affectedKeys.add(key);
         }
         return null;
      }
   }

   @Override
   @ManagedOperation(
         description = "Resets statistics gathered by this component",
         displayName = "Reset statistics"
   )
   public void resetStatistics() {
      cacheStores.set(0);
   }

   @ManagedAttribute(
         description = "number of cache loader stores",
         displayName = "Number of cache stores",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getCacheLoaderStores() {
      return cacheStores.get();
   }

   InternalCacheEntry getStoredEntry(Object key, InvocationContext ctx) {
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry instanceof InternalCacheEntry) {
         return (InternalCacheEntry) entry;
      } else {
         return entryFactory.create(entry);
      }
   }

   public void disableInterceptor() {
      enabled = false;
   }

   public Map<GlobalTransaction, Set<Object>> getPreparingTxs() {
      return Collections.unmodifiableMap(preparingTxs);
   }

   public Map<GlobalTransaction, Integer> getTxStores() {
      return Collections.unmodifiableMap(txStores);
   }
}
