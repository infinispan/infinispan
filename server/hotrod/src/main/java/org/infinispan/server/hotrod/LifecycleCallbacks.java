package org.infinispan.server.hotrod;

import static org.infinispan.server.core.ExternalizerIds.BINARY_CONVERTER;
import static org.infinispan.server.core.ExternalizerIds.BINARY_FILTER;
import static org.infinispan.server.core.ExternalizerIds.BINARY_FILTER_CONVERTER;
import static org.infinispan.server.core.ExternalizerIds.CACHE_XID;
import static org.infinispan.server.core.ExternalizerIds.ITERATION_FILTER;
import static org.infinispan.server.core.ExternalizerIds.KEY_VALUE_VERSION_CONVERTER;
import static org.infinispan.server.core.ExternalizerIds.KEY_VALUE_WITH_PREVIOUS_CONVERTER;
import static org.infinispan.server.core.ExternalizerIds.SERVER_ADDRESS;
import static org.infinispan.server.core.ExternalizerIds.TX_FUNCTIONS;
import static org.infinispan.server.core.ExternalizerIds.TX_STATE;

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
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.hotrod.ClientListenerRegistry.UnmarshallConverterExternalizer;
import org.infinispan.server.hotrod.ClientListenerRegistry.UnmarshallFilterConverterExternalizer;
import org.infinispan.server.hotrod.ClientListenerRegistry.UnmarshallFilterExternalizer;
import org.infinispan.server.hotrod.event.KeyValueWithPreviousEventConverterExternalizer;
import org.infinispan.server.hotrod.iteration.IterationFilter;
import org.infinispan.server.hotrod.tx.CacheXid;
import org.infinispan.server.hotrod.tx.ServerTransactionOriginatorChecker;
import org.infinispan.server.hotrod.tx.ServerTransactionTable;
import org.infinispan.server.hotrod.tx.TxFunctions;
import org.infinispan.server.hotrod.tx.TxState;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.TransactionOriginatorChecker;
import org.infinispan.util.ByteString;

/**
 * Module lifecycle callbacks implementation that enables module specific {@link AdvancedExternalizer}
 * implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class LifecycleCallbacks extends AbstractModuleLifecycle {

   /**
    * Cache name to store the global transaction table. It contains the state of the client transactions.
    */
   private static final String GLOBAL_TX_TABLE_CACHE_NAME = "__global_tx_table__";

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      Map<Integer, AdvancedExternalizer<?>> externalizers = globalCfg.serialization().advancedExternalizers();
      externalizers.put(SERVER_ADDRESS, new ServerAddress.Externalizer());
      externalizers.put(BINARY_FILTER, new UnmarshallFilterExternalizer());
      externalizers.put(BINARY_CONVERTER, new UnmarshallConverterExternalizer());
      externalizers.put(KEY_VALUE_VERSION_CONVERTER, new KeyValueVersionConverter.Externalizer());
      externalizers.put(BINARY_FILTER_CONVERTER, new UnmarshallFilterConverterExternalizer());
      externalizers.put(KEY_VALUE_WITH_PREVIOUS_CONVERTER, new KeyValueWithPreviousEventConverterExternalizer());
      externalizers.put(ITERATION_FILTER, new IterationFilter.IterationFilterExternalizer());
      externalizers.put(TX_STATE, TxState.EXTERNALIZER);
      externalizers.put(CACHE_XID, CacheXid.EXTERNALIZER);
      externalizers.put(TX_FUNCTIONS, TxFunctions.EXTERNALIZER);
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
    * Registers the {@link ServerTransactionTable} to a transactional cache.
    */
   private void registerServerTransactionTable(ComponentRegistry componentRegistry, String cacheName) {
      //skip for global tx table and non-transactional cache
      if (GLOBAL_TX_TABLE_CACHE_NAME.equals(cacheName) ||
            !componentRegistry.getComponent(Configuration.class).transaction().transactionMode().isTransactional()) {
         return;
      }
      EmbeddedCacheManager cacheManager = componentRegistry.getGlobalComponentRegistry()
            .getComponent(EmbeddedCacheManager.class);
      Cache<CacheXid, TxState> globalTxTable = cacheManager.getCache(GLOBAL_TX_TABLE_CACHE_NAME);
      ServerTransactionTable transactionTable = new ServerTransactionTable(globalTxTable,
            ByteString.fromString(cacheName));
      componentRegistry.registerComponent(transactionTable, ServerTransactionTable.class);
      ServerTransactionOriginatorChecker checker = new ServerTransactionOriginatorChecker(globalTxTable);
      componentRegistry.registerComponent(checker, TransactionOriginatorChecker.class);
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


}
