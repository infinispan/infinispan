package org.infinispan.query.impl.massindex;

import static org.infinispan.query.impl.massindex.MassIndexStrategyFactory.calculateStrategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.impl.massindex.MassIndexStrategy.CleanExecutionMode;
import org.infinispan.query.impl.massindex.MassIndexStrategy.FlushExecutionMode;
import org.infinispan.query.impl.massindex.MassIndexStrategy.IndexingExecutionMode;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.logging.LogFactory;

/**
 * @author gustavonalle
 * @since 7.1
 */
@MBean(objectName = "MassIndexer",
      description = "Component that rebuilds the Lucene index from the cached data")
public class DistributedExecutorMassIndexer implements MassIndexer {

   private static final Log LOG = LogFactory.getLog(DistributedExecutorMassIndexer.class, Log.class);

   private final AdvancedCache<?, ?> cache;
   private final SearchIntegrator searchIntegrator;
   private final IndexUpdater indexUpdater;
   private final ClusterExecutor executor;
   private final ExecutorService localExecutor;
   private final MassIndexLock lock;

   private static final TriConsumer<Address, Void, Throwable> TRI_CONSUMER = (a, v, t) -> {
      if (t != null) {
         throw new CacheException(t);
      }
   };

   public DistributedExecutorMassIndexer(AdvancedCache cache, SearchIntegrator searchIntegrator,
                                         KeyTransformationHandler keyTransformationHandler, TimeService timeService) {
      this.cache = cache;
      this.searchIntegrator = searchIntegrator;
      this.indexUpdater = new IndexUpdater(searchIntegrator, keyTransformationHandler, timeService);
      this.executor = cache.getCacheManager().executor();
      this.localExecutor = cache.getCacheManager().getGlobalComponentRegistry()
            .getComponent(ExecutorService.class, KnownComponentNames.PERSISTENCE_EXECUTOR);
      this.lock = MassIndexerLockFactory.buildLock(cache);
   }

   @ManagedOperation(description = "Starts rebuilding the index", displayName = "Rebuild index")
   @Override
   public void start() {
      CompletionStages.join(executeInternal(false));
   }

   @Override
   public CompletableFuture<Void> purge() {
      return executeInternal(true);
   }

   @Override
   public CompletableFuture<Void> startAsync() {
      return executeInternal(false);
   }

   @Override
   public CompletableFuture<Void> reindex(Object... keys) {
      CompletableFuture<Void> future = null;
      Set<Object> everywhereSet = new HashSet<>();
      Set<Object> primeownerSet = new HashSet<>();
      for (Object key : keys) {
         if (cache.containsKey(key)) {
            Class<?> indexedType = cache.get(key).getClass();
            EntityIndexBinding indexBinding = searchIntegrator.getIndexBindings().get(new PojoIndexedTypeIdentifier(indexedType));
            MassIndexStrategy strategy = calculateStrategy(indexBinding, cache.getCacheConfiguration());
            IndexingExecutionMode indexingStrategy = strategy.getIndexingStrategy();
            switch (indexingStrategy) {
               case ALL:
                  everywhereSet.add(key);
                  break;
               case PRIMARY_OWNER:
                  primeownerSet.add(key);
                  break;
            }
         } else {
            LOG.warn("cache contains no mapping for the key");
         }
      }
      TriConsumer<Address, Void, Throwable> triConsumer = (a, v, t) -> {
         if (t != null) {
            throw new CacheException(t);
         }
      };
      if (everywhereSet.size() > 0) {
         IndexWorker indexWorkEverywhere =
               new IndexWorker(cache.getName(), null, false, false, false, false, everywhereSet);

         future = executor.submitConsumer(indexWorkEverywhere, triConsumer);
      }
      if (primeownerSet.size() > 0) {
         Map<Address, Set<Object>> targets = new HashMap<>();
         DistributionManager distributionManager = cache.getDistributionManager();
         if (distributionManager != null) {
            LocalizedCacheTopology localizedCacheTopology = cache.getDistributionManager().getCacheTopology();
            for (Object key : primeownerSet) {
               Address primary = localizedCacheTopology.getDistribution(key).primary();
               Set<Object> keysForAddress = targets.get(primary);
               if (keysForAddress == null) {
                  keysForAddress = new HashSet<>();
                  targets.put(primary, keysForAddress);
               }
               keysForAddress.add(key);
            }
            List<CompletableFuture<Void>> futures;
            if (future != null) {
               futures = new ArrayList<>(targets.size() + 1);
               futures.add(future);
            } else {
               futures = new ArrayList<>(targets.size());
            }
            for (Map.Entry<Address, Set<Object>> entry : targets.entrySet()) {
               IndexWorker indexWorkEverywhere =
                     new IndexWorker(cache.getName(), null, false, false, false, false, entry.getValue());

               // TODO: need to change this to not index
               futures.add(executor.filterTargets(Collections.singleton(entry.getKey())).submitConsumer(indexWorkEverywhere, triConsumer));
            }
            future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
         } else {
            // This is a local only cache with no distribution manager
            IndexWorker indexWorkEverywhere =
                  new IndexWorker(cache.getName(), null, false, false, false, false, primeownerSet);
            CompletableFuture<Void> localFuture = executor.submitConsumer(indexWorkEverywhere, triConsumer);
            if (future != null) {
               future = CompletableFuture.allOf(future, localFuture);
            } else {
               future = localFuture;
            }
         }
      }
      return future != null ? future : CompletableFutures.completedNull();
   }

   @Override
   public boolean isRunning() {
      return lock.isAcquired();
   }

   private CompletableFuture<Void> executeInternal(boolean skipIndex) {
      if (lock.lock()) {
         List<CompletableFuture<Void>> futures = new ArrayList<>();
         Deque<IndexedTypeIdentifier> toFlush = new LinkedList<>();

         BiConsumer<Void, Throwable> flushIfNeeded = (v, t) -> {
            try {
               for (IndexedTypeIdentifier type : toFlush) {
                  indexUpdater.flush(type);
               }
            } finally {
               lock.unlock();
            }
         };
         CompletableFuture<Void> compositeFuture;
         try {
            for (IndexedTypeIdentifier indexedType : searchIntegrator.getIndexBindings().keySet()) {
               EntityIndexBinding indexBinding = searchIntegrator.getIndexBindings().get(indexedType);
               MassIndexStrategy strategy = calculateStrategy(indexBinding, cache.getCacheConfiguration());
               boolean workerClean = true, workerFlush = true;
               if (strategy.getCleanStrategy() == CleanExecutionMode.ONCE_BEFORE) {
                  indexUpdater.purge(indexedType);
                  workerClean = false;
               }
               if (strategy.getFlushStrategy() == FlushExecutionMode.ONCE_AFTER) {
                  toFlush.add(indexedType);
                  workerFlush = false;
               }

            IndexingExecutionMode indexingStrategy = strategy.getIndexingStrategy();
            IndexWorker indexWork = new IndexWorker(cache.getName(), indexedType, workerFlush, workerClean,
                  skipIndex, indexingStrategy == IndexingExecutionMode.PRIMARY_OWNER, null);

               futures.add(executor.submitConsumer(indexWork, TRI_CONSUMER));
            }
            compositeFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            return compositeFuture.whenCompleteAsync(flushIfNeeded, localExecutor);
         } catch (Throwable t) {
            lock.unlock();
            return CompletableFutures.completedExceptionFuture(t);
         }
      } else {
         return CompletableFutures.completedExceptionFuture(new MassIndexerAlreadyStartedException());
      }
   }
}
