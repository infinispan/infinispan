package org.infinispan.server.hotrod;

import java.util.EnumSet;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.hotrod.tx.ServerTransactionOriginatorChecker;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.infinispan.server.hotrod.tx.table.PerCacheTxTable;
import org.infinispan.server.hotrod.tx.table.TxState;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.TransactionOriginatorChecker;

import net.jcip.annotations.GuardedBy;

/**
 * @author Galder Zamarreño
 * @since 5.0
 */
@InfinispanModule(name = "server-hotrod", requiredModules = "core")
public class LifecycleCallbacks implements ModuleLifecycle {

   /**
    * Cache name to store the global transaction table. It contains the state of the client transactions.
    */
   public static final String GLOBAL_TX_TABLE_CACHE_NAME = "org.infinispan.CLIENT_SERVER_TX_TABLE";

   @GuardedBy("this")
   private boolean registered = false;
   private GlobalComponentRegistry globalComponentRegistry;
   private GlobalConfiguration globalCfg;

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      globalComponentRegistry = gcr;
      this.globalCfg = globalCfg;

      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.GLOBAL, new GlobalContextInitializerImpl());

      registerGlobalTxTable();
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      // It's too late to register components here, internal caches have already started
   }

   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
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
      EmbeddedCacheManager cacheManager = globalComponentRegistry.getComponent(EmbeddedCacheManager.class);
      createGlobalTxTable(cacheManager);
      // TODO We need a way for a module to install a factory before the default implementation is instantiated
      BasicComponentRegistry basicComponentRegistry = componentRegistry.getComponent(BasicComponentRegistry.class);
      basicComponentRegistry.replaceComponent(PerCacheTxTable.class.getName(), new PerCacheTxTable(cacheManager.getAddress()), true);
      basicComponentRegistry.replaceComponent(TransactionOriginatorChecker.class.getName(), new ServerTransactionOriginatorChecker(), true);
      componentRegistry.rewire();
   }

   /**
    * Creates the global transaction internal cache.
    */
   private void registerGlobalTxTable() {
      InternalCacheRegistry registry = globalComponentRegistry.getComponent(InternalCacheRegistry.class);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      //we can't lose transactions. distributed cache can lose data is num_owner nodes crash at the same time
      // If this cache is changed from REPL/LOCAL it will also become blocking - and the blockhound exception in
      // ServerHotrodBlockHoundIntegration will no longer be correct
      builder.clustering().cacheMode(globalCfg.isClustered() ?
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
         GlobalTxTable txTable = new GlobalTxTable(cache, globalComponentRegistry);
         globalComponentRegistry.registerComponent(txTable, GlobalTxTable.class);
         registered = true;

      }
   }


}
