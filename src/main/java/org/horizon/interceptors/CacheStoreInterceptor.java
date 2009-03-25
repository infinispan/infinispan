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
import org.horizon.commands.VisitableCommand;
import org.horizon.commands.tx.CommitCommand;
import org.horizon.commands.tx.PrepareCommand;
import org.horizon.commands.tx.RollbackCommand;
import org.horizon.commands.write.ClearCommand;
import org.horizon.commands.write.PutKeyValueCommand;
import org.horizon.commands.write.PutMapCommand;
import org.horizon.commands.write.RemoveCommand;
import org.horizon.commands.write.ReplaceCommand;
import org.horizon.commands.write.WriteCommand;
import org.horizon.config.CacheLoaderManagerConfig;
import org.horizon.container.entries.CacheEntry;
import org.horizon.container.entries.InternalCacheEntry;
import org.horizon.container.entries.InternalEntryFactory;
import org.horizon.context.InvocationContext;
import org.horizon.context.TransactionContext;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.Start;
import org.horizon.interceptors.base.JmxStatsCommandInterceptor;
import org.horizon.invocation.Flag;
import org.horizon.jmx.annotations.ManagedAttribute;
import org.horizon.jmx.annotations.ManagedOperation;
import org.horizon.loader.CacheLoaderManager;
import org.horizon.loader.CacheStore;
import org.horizon.loader.modifications.Clear;
import org.horizon.loader.modifications.Modification;
import org.horizon.loader.modifications.Remove;
import org.horizon.loader.modifications.Store;
import org.horizon.logging.LogFactory;
import org.horizon.transaction.GlobalTransaction;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.HashMap;
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
public class CacheStoreInterceptor extends JmxStatsCommandInterceptor {
   private CacheLoaderManagerConfig loaderConfig = null;
   private TransactionManager txMgr = null;
   private HashMap<Transaction, Integer> txStores = new HashMap<Transaction, Integer>();
   private Map<Transaction, Set<Object>> preparingTxs = new ConcurrentHashMap<Transaction, Set<Object>>();
   private final AtomicLong cacheStores = new AtomicLong(0);
   CacheStore store;
   private CacheLoaderManager loaderManager;
   private boolean statsEnabled;

   public CacheStoreInterceptor() {
      log = LogFactory.getLog(getClass());
      trace = log.isTraceEnabled();
   }

   @Inject
   protected void init(CacheLoaderManager loaderManager, TransactionManager txManager) {
      this.loaderManager = loaderManager;
      txMgr = txManager;
   }

   @Start(priority = 15)
   protected void start() {
      store = loaderManager.getCacheStore();
      this.setStatisticsEnabled(configuration.isExposeJmxStatistics());
      loaderConfig = configuration.getCacheLoaderManagerConfig();
   }

   /**
    * if this is a shared cache loader and the call is of remote origin, pass up the chain
    */
   public final boolean skip(InvocationContext ctx, VisitableCommand command) {
      if (store == null) return true;  // could be because the cache loader oes not implement cache store
      if ((!ctx.isOriginLocal() && loaderConfig.isShared()) || ctx.hasFlag(Flag.SKIP_CACHE_STORE)) {
         if (trace)
            log.trace("Passing up method call and bypassing this interceptor since the cache loader is shared and this call originated remotely.");
         return true;
      }
      return false;
   }

   @Override
   public Object visitCommitCommand(InvocationContext ctx, CommitCommand command) throws Throwable {
      if (!skip(ctx, command) && inTransaction()) {
         if (ctx.getTransactionContext().hasAnyModifications()) {
            // this is a commit call.
            Transaction tx = ctx.getTransaction();
            log.trace("Calling loader.commit() for transaction {0}", tx);
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
   public Object visitRollbackCommand(InvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!skip(ctx, command) && inTransaction()) {
         if (trace) log.trace("transactional so don't put stuff in the cloader yet.");
         if (ctx.getTransactionContext().hasAnyModifications()) {
            Transaction tx = ctx.getTransaction();
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
   public Object visitPrepareCommand(InvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!skip(ctx, command) && inTransaction()) {
         if (trace) log.trace("transactional so don't put stuff in the cloader yet.");
         prepareCacheLoader(ctx, command.getGlobalTransaction(), ctx.getTransactionContext(), command.isOnePhaseCommit());
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      if (!skip(ctx, command) && !inTransaction() && command.isSuccessful()) {
         Object key = command.getKey();
         boolean resp = store.remove(key);
         log.trace("Removed entry under key {0} and got response {1} from CacheStore", key, resp);
      }
      return retval;
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (!skip(ctx, command) && !inTransaction()) {
         store.clear();
         log.trace("Cleared cache store");
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (skip(ctx, command) || inTransaction() || !command.isSuccessful()) return returnValue;

      Object key = command.getKey();
      InternalCacheEntry se = getStoredEntry(key, ctx);
      store.store(se);
      log.trace("Stored entry {0} under key {1}", se, key);
      if (getStatisticsEnabled()) cacheStores.incrementAndGet();

      return returnValue;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (skip(ctx, command) || inTransaction() || !command.isSuccessful()) return returnValue;

      Object key = command.getKey();
      InternalCacheEntry se = getStoredEntry(key, ctx);
      store.store(se);
      log.trace("Stored entry {0} under key {1}", se, key);
      if (getStatisticsEnabled()) cacheStores.incrementAndGet();

      return returnValue;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (skip(ctx, command) || inTransaction()) return returnValue;

      Map<Object, Object> map = command.getMap();
      for (Object key : map.keySet()) {
         InternalCacheEntry se = getStoredEntry(key, ctx);
         store.store(se);
         log.trace("Stored entry {0} under key {1}", se, key);
      }
      if (getStatisticsEnabled()) cacheStores.getAndAdd(map.size());
      return returnValue;
   }

   private boolean inTransaction() throws SystemException {
      return txMgr != null && txMgr.getTransaction() != null;
   }

   private void prepareCacheLoader(InvocationContext ctx, GlobalTransaction gtx, TransactionContext transactionContext, boolean onePhase) throws Throwable {
      if (transactionContext == null) {
         throw new Exception("transactionContext for transaction " + gtx + " not found in transaction table");
      }
      List<WriteCommand> modifications = transactionContext.getModifications();
      if (modifications.size() == 0) {
         log.trace("Transaction has not logged any modifications!");
         return;
      }
      log.trace("Cache loader modification list: {0}", modifications);
      StoreModificationsBuilder modsBuilder = new StoreModificationsBuilder(getStatisticsEnabled());
      for (WriteCommand cacheCommand : modifications) cacheCommand.acceptVisitor(ctx, modsBuilder);
      int numMods = modsBuilder.modifications.size();
      log.trace("Converted method calls to cache loader modifications.  List size: {0}", numMods);

      if (numMods > 0) {
         Transaction tx = transactionContext.getTransaction();
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

   @ManagedOperation
   public void resetStatistics() {
      cacheStores.set(0);
   }

   @ManagedAttribute
   public boolean getStatisticsEnabled() {
      return statsEnabled;
   }

   @ManagedAttribute
   public void setStatisticsEnabled(boolean enabled) {
      this.statsEnabled = enabled;
   }

   @ManagedAttribute(description = "number of cache loader stores")
   public long getCacheLoaderStores() {
      return cacheStores.get();
   }

   private InternalCacheEntry getStoredEntry(Object key, InvocationContext ctx) {
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry instanceof InternalCacheEntry) {
         return (InternalCacheEntry) entry;
      } else {
         return InternalEntryFactory.create(entry.getKey(), entry.getValue(), entry.getLifespan(), entry.getMaxIdle());
      }
   }
}
