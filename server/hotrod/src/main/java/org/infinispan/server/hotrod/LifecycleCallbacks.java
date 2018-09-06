package org.infinispan.server.hotrod;

import static org.infinispan.server.core.ExternalizerIds.CACHE_XID;
import static org.infinispan.server.core.ExternalizerIds.CLIENT_ADDRESS;
import static org.infinispan.server.core.ExternalizerIds.COMPLETE_FUNCTION;
import static org.infinispan.server.core.ExternalizerIds.CONDITIONAL_MARK_ROLLBACK_FUNCTION;
import static org.infinispan.server.core.ExternalizerIds.CREATE_STATE_FUNCTION;
import static org.infinispan.server.core.ExternalizerIds.DECISION_FUNCTION;
import static org.infinispan.server.core.ExternalizerIds.ITERATION_FILTER;
import static org.infinispan.server.core.ExternalizerIds.KEY_VALUE_VERSION_CONVERTER;
import static org.infinispan.server.core.ExternalizerIds.KEY_VALUE_WITH_PREVIOUS_CONVERTER;
import static org.infinispan.server.core.ExternalizerIds.NEAR_CACHE_FILTER_CONVERTER;
import static org.infinispan.server.core.ExternalizerIds.PREPARED_FUNCTION;
import static org.infinispan.server.core.ExternalizerIds.PREPARING_FUNCTION;
import static org.infinispan.server.core.ExternalizerIds.SERVER_ADDRESS;
import static org.infinispan.server.core.ExternalizerIds.TX_STATE;
import static org.infinispan.server.core.ExternalizerIds.XID_PREDICATE;

import java.util.EnumSet;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.hotrod.event.KeyValueWithPreviousEventConverterExternalizer;
import org.infinispan.server.hotrod.iteration.IterationFilter;
import org.infinispan.server.hotrod.tx.ServerTransactionOriginatorChecker;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.ClientAddress;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.infinispan.server.hotrod.tx.table.PerCacheTxTable;
import org.infinispan.server.hotrod.tx.table.TxState;
import org.infinispan.server.hotrod.tx.table.functions.ConditionalMarkAsRollbackFunction;
import org.infinispan.server.hotrod.tx.table.functions.CreateStateFunction;
import org.infinispan.server.hotrod.tx.table.functions.PreparingDecisionFunction;
import org.infinispan.server.hotrod.tx.table.functions.SetCompletedTransactionFunction;
import org.infinispan.server.hotrod.tx.table.functions.SetDecisionFunction;
import org.infinispan.server.hotrod.tx.table.functions.SetPreparedFunction;
import org.infinispan.server.hotrod.tx.table.functions.XidPredicate;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.TransactionOriginatorChecker;

import net.jcip.annotations.GuardedBy;

/**
 * Module lifecycle callbacks implementation that enables module specific {@link AdvancedExternalizer} implementations
 * to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class LifecycleCallbacks implements ModuleLifecycle {

   /**
    * Cache name to store the global transaction table. It contains the state of the client transactions.
    */
   private static final String GLOBAL_TX_TABLE_CACHE_NAME = "org.infinispan.CLIENT_SERVER_TX_TABLE";

   @GuardedBy("this")
   private boolean registered = false;

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      Map<Integer, AdvancedExternalizer<?>> externalizers = globalCfg.serialization().advancedExternalizers();
      externalizers.put(SERVER_ADDRESS, new ServerAddress.Externalizer());
      externalizers.put(KEY_VALUE_VERSION_CONVERTER, new KeyValueVersionConverter.Externalizer());
      externalizers.put(NEAR_CACHE_FILTER_CONVERTER, new KeyOnlyFilterConverter.Externalizer());
      externalizers.put(KEY_VALUE_WITH_PREVIOUS_CONVERTER, new KeyValueWithPreviousEventConverterExternalizer());
      externalizers.put(ITERATION_FILTER, new IterationFilter.IterationFilterExternalizer());
      externalizers.put(TX_STATE, TxState.EXTERNALIZER);
      externalizers.put(CACHE_XID, CacheXid.EXTERNALIZER);
      externalizers.put(CLIENT_ADDRESS, ClientAddress.EXTERNALIZER);
      externalizers.put(CREATE_STATE_FUNCTION, CreateStateFunction.EXTERNALIZER);
      externalizers.put(PREPARING_FUNCTION, PreparingDecisionFunction.EXTERNALIZER);
      externalizers.put(COMPLETE_FUNCTION, SetCompletedTransactionFunction.EXTERNALIZER);
      externalizers.put(DECISION_FUNCTION, SetDecisionFunction.EXTERNALIZER);
      externalizers.put(PREPARED_FUNCTION, SetPreparedFunction.EXTERNALIZER);
      externalizers.put(XID_PREDICATE, XidPredicate.EXTERNALIZER);
      externalizers.put(CONDITIONAL_MARK_ROLLBACK_FUNCTION, ConditionalMarkAsRollbackFunction.EXTERNALIZER);
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      registerGlobalTxTable(gcr);
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      registerServerTransactionTable(cr, cacheName);
   }

   /**
    * Registers the {@link PerCacheTxTable} to a transactional cache.
    */
   private void registerServerTransactionTable(ComponentRegistry componentRegistry, String cacheName) {
      //skip for global tx table and non-transactional cache
      if (GLOBAL_TX_TABLE_CACHE_NAME.equals(cacheName) ||
            !componentRegistry.getComponent(Configuration.class).transaction().transactionMode().isTransactional()) {
         return;
      }
      EmbeddedCacheManager cacheManager = componentRegistry.getGlobalComponentRegistry()
            .getComponent(EmbeddedCacheManager.class);
      createGlobalTxTable(cacheManager);
      componentRegistry.registerComponent(new PerCacheTxTable(cacheManager.getAddress()), PerCacheTxTable.class);
      componentRegistry.registerComponent(new ServerTransactionOriginatorChecker(), TransactionOriginatorChecker.class);
      componentRegistry.rewire();
   }

   /**
    * Creates the global transaction internal cache.
    */
   private void registerGlobalTxTable(GlobalComponentRegistry globalComponentRegistry) {
      InternalCacheRegistry registry = globalComponentRegistry.getComponent(InternalCacheRegistry.class);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      //we can't lose transactions. distributed cache can lose data is num_owner nodes crash at the same time
      builder.clustering().cacheMode(globalComponentRegistry.getGlobalConfiguration().isClustered() ?
            CacheMode.REPL_SYNC :
            CacheMode.LOCAL);
      builder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      //persistent? should we keep the transaction after restart?
      registry.registerInternalCache(GLOBAL_TX_TABLE_CACHE_NAME, builder.build(),
            EnumSet.noneOf(InternalCacheRegistry.Flag.class));
   }

   private synchronized void createGlobalTxTable(EmbeddedCacheManager cacheManager) {
      if (!registered) {
         Cache<CacheXid, TxState> cache = cacheManager.getCache(GLOBAL_TX_TABLE_CACHE_NAME);
         GlobalTxTable txTable = new GlobalTxTable(cache, cacheManager.getGlobalComponentRegistry());
         cacheManager.getGlobalComponentRegistry().registerComponent(txTable, GlobalTxTable.class);
         registered = true;

      }
   }


}
