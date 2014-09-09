package org.infinispan.registry.impl;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.filter.KeyFilter;
import org.infinispan.registry.ClusterRegistry;
import org.infinispan.registry.ScopedKey;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.InvalidTransactionException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of the ClusterRegistry. Stores all the information in a replicated cache that is lazily
 * instantiated on the first access. This means that if the EmbeddedCacheManager doesn't use the metadata the
 * underlying cache never gets instantiated.
 *
 * @author Mircea Markus
 * @since 6.0
 */
@ThreadSafe
public class ClusterRegistryImpl<S, K, V> implements ClusterRegistry<S, K, V> {

   public static final String GLOBAL_REGISTRY_CACHE_NAME = "__cluster_registry_cache__";

   private static final Log log = LogFactory.getLog(ClusterRegistryImpl.class);

   private EmbeddedCacheManager cacheManager;
   private volatile Cache<ScopedKey<S, K>, V> clusterRegistryCache;
   private volatile AdvancedCache<ScopedKey<S, K>, V> clusterRegistryCacheWithoutReturn;
   private volatile TransactionManager transactionManager;

   @Inject
   public void init(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Stop(priority=1)
   public void stop() {
      if (clusterRegistryCache != null) {
         clusterRegistryCache.stop();
      }
      clusterRegistryCache = null;
   }

   protected void runCommand(Runnable runnable) {
      while (true) {
         try {
            runnable.run();
            break;
         } catch (CacheException e) {
            if (SuspectException.isSuspectExceptionInChain(e)) {
               // Retry the command
               log.trace("Ignoring suspect exception and retrying operation for ClusterRegistry.");
            } else {
               throw e;
            }
         }
      }
   }

   @Override
   public void put(final S scope, final K key, final V value) {
      if (value == null) throw new IllegalArgumentException("Null values are not allowed");
      startRegistryCache();
      Transaction tx = suspendTx();
      try {
         runCommand(new Runnable() {

            @Override
            public void run() {
               clusterRegistryCacheWithoutReturn.put(new ScopedKey<S, K>(scope, key), value);
            }
         });
      } finally {
         resumeTx(tx);
      }
   }

   @Override
   public void put(final S scope, final K key, final V value, final long lifespan, final TimeUnit unit) {
      if (value == null) throw new IllegalArgumentException("Null values are not allowed");
      startRegistryCache();
      Transaction tx = suspendTx();
      try {
         runCommand(new Runnable() {

            @Override
            public void run() {
               clusterRegistryCacheWithoutReturn.put(new ScopedKey<S, K>(scope, key), value, lifespan, unit);
            }
         });
      } finally {
         resumeTx(tx);
      }
   }

   @Override
   public void remove(final S scope, final K key) {
      startRegistryCache();
      Transaction tx = suspendTx();
      try {
         runCommand(new Runnable() {

            @Override
            public void run() {
               clusterRegistryCacheWithoutReturn.remove(new ScopedKey<S, K>(scope, key));
            }
         });
      } finally {
         resumeTx(tx);
      }
   }

   @Override
   public V get(S scope, K key) {
      startRegistryCache();
      // We don't want repeatable read semantics for the cluster registry, so we need to suspend for reads as well
      Transaction tx = suspendTx();
      try {
         return clusterRegistryCache.get(new ScopedKey<S, K>(scope, key));
      } finally {
         resumeTx(tx);
      }
   }

   @Override
   public boolean containsKey(S scope, K key) {
      startRegistryCache();
      Transaction tx = suspendTx();
      try {
         return clusterRegistryCache.containsKey(new ScopedKey<S, K>(scope, key));
      } finally {
         resumeTx(tx);
      }
   }

   @Override
   public Set<K> keys(S scope) {
      startRegistryCache();
      Set<K> result = new HashSet<K>();
      Transaction tx = suspendTx();
      try {
         for (ScopedKey<S, K> key : clusterRegistryCache.keySet()) {
            if (key.hasScope(scope)) {
               result.add(key.getKey());
            }
         }
         return result;
      } finally {
         resumeTx(tx);
      }
   }

   @Override
   public void clear(S scope) {
      startRegistryCache();
      Transaction tx = suspendTx();
      try {
         for (final ScopedKey<S, K> key : clusterRegistryCache.keySet()) {
            if (key.hasScope(scope)) {
               runCommand(new Runnable() {

                  @Override
                  public void run() {
                     clusterRegistryCacheWithoutReturn.remove(key);
                  }
               });
            }
         }
      } finally {
         resumeTx(tx);
      }
   }

   @Override
   public void clearAll() {
      startRegistryCache();
      Transaction tx = suspendTx();
      try {
         runCommand(new Runnable() {

            @Override
            public void run() {
               clusterRegistryCache.clear();
            }
         });
      } finally {
         resumeTx(tx);
      }
   }

   @Override
   public void addListener(final S scope, final Object listener) {
      startRegistryCache();
      clusterRegistryCache.addListener(listener, new KeyFilter() {
         @Override
         public boolean accept(Object key) {
            // All keys are known to be of type ScopedKey
            ScopedKey<S, K> scopedKey = (ScopedKey<S, K>) key;
            return scopedKey.hasScope(scope);
         }
      });
   }

   @Override
   public void addListener(final S scope, final KeyFilter keyFilter, final Object listener) {
      startRegistryCache();
      clusterRegistryCache.addListener(listener, new KeyFilter() {
         @Override
         public boolean accept(Object key) {
            // All keys are known to be of type ScopedKey
            ScopedKey<S, K> scopedKey = (ScopedKey<S, K>) key;
            return scopedKey.hasScope(scope) && keyFilter.accept(scopedKey.getKey());
         }
      });
   }

   @Override
   public void removeListener(Object listener) {
      if (clusterRegistryCache != null) {
         clusterRegistryCache.removeListener(listener);
      }
   }

   /**
    * Start the cache lazily: if the cluster registry is not needed at all then this won't ever be started.
    */
   private void startRegistryCache() {
      if (clusterRegistryCache == null) {
         synchronized (this) {
            if (clusterRegistryCache != null) return;
            SecurityActions.defineConfiguration(cacheManager, GLOBAL_REGISTRY_CACHE_NAME, getRegistryCacheConfig());
            clusterRegistryCache = SecurityActions.getRegistryCache(cacheManager);
            clusterRegistryCacheWithoutReturn = clusterRegistryCache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);
            transactionManager = clusterRegistryCacheWithoutReturn.getTransactionManager();
         }
      }
   }

   private Configuration getRegistryCacheConfig() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

      //allow the registry to work for local caches as well as clustered caches
      CacheMode cacheMode = isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;
      configurationBuilder.clustering().cacheMode(cacheMode);

      // use invocation batching (cache-only transactions) for high consistency as writes are expected to be rare in this cache
      configurationBuilder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(null).invocationBatching().enable();

      //fetch the state (redundant as state transfer this is enabled by default, keep it here to document the intention)
      configurationBuilder.clustering().stateTransfer().fetchInMemoryState(true);

      return configurationBuilder.build();
   }

   private boolean isClustered() {
      GlobalConfiguration globalConfiguration = cacheManager.getGlobalComponentRegistry().getGlobalConfiguration();
      return globalConfiguration.isClustered();
   }

   /**
    * Suspend any ongoing transaction, so that the cluster registry writes are committed immediately.
    */
   private Transaction suspendTx() {
      try {
         if (transactionManager == null) {
            return null;
         }
         return transactionManager.suspend();
      } catch (SystemException e) {
         throw new CacheException("Unable to suspend ongoing transaction", e);
      }
   }

   private void resumeTx(Transaction tx) {
      try {
         if (tx != null) {
            transactionManager.resume(tx);
         }
      } catch (InvalidTransactionException | SystemException e) {
         throw new CacheException("Unable to resume ongoing transaction", e);
      }
   }

}
