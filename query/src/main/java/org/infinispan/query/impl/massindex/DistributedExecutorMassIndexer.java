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
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
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
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.impl.massindex.MassIndexStrategy.CleanExecutionMode;
import org.infinispan.query.impl.massindex.MassIndexStrategy.FlushExecutionMode;
import org.infinispan.query.impl.massindex.MassIndexStrategy.IndexingExecutionMode;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.logging.LogFactory;

/**
 * @author gustavonalle
 * @since 7.1
 */
public class DistributedExecutorMassIndexer implements MassIndexer {

   private static final Log LOG = LogFactory.getLog(DistributedExecutorMassIndexer.class, Log.class);

   private final AdvancedCache<?, ?> cache;
   private final SearchIntegrator searchIntegrator;
   private final IndexUpdater indexUpdater;
   private final ClusterExecutor executor;

   public DistributedExecutorMassIndexer(AdvancedCache cache, SearchIntegrator searchIntegrator,
                                         KeyTransformationHandler keyTransformationHandler, TimeService timeService) {
      this.cache = cache;
      this.searchIntegrator = searchIntegrator;
      this.indexUpdater = new IndexUpdater(searchIntegrator, keyTransformationHandler, timeService);
      this.executor = cache.getCacheManager().executor();
   }

   @Override
   public void start() {
      CompletableFuture<?> executionResult = executeInternal(false);
      executionResult.join();
   }

   @Override
   public CompletableFuture<Void> startAsync() {
      return executeInternal(true);
   }

   private void addFutureListToFutures(List<CompletableFuture<Void>> futures, List<CompletableFuture<Void>> futureList) {
      futureList.forEach(f -> futures.add(f.exceptionally(t -> {
         if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return null;
         } else if (t instanceof CompletionException) {
            Throwable cause = t.getCause();
            throw LOG.errorExecutingMassIndexer(cause);
         } else {
            throw LOG.errorExecutingMassIndexer(t);
         }
      })));
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
               new IndexWorker(cache.getName(),null, false, false, false, everywhereSet);

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
                     new IndexWorker(cache.getName(),null, false, false, false, entry.getValue());

               // TODO: need to change this to not index
               futures.add(executor.filterTargets(Collections.singleton(entry.getKey())).submitConsumer(indexWorkEverywhere, triConsumer));
            }
            future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
         } else {
            // This is a local only cache with no distribution manager
            IndexWorker indexWorkEverywhere =
                  new IndexWorker(cache.getName(),null, false, false, false, primeownerSet);
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

   private CompletableFuture<Void> executeInternal(boolean asyncFlush) {
      List<CompletableFuture<Void>> futures = new ArrayList<>();
      Deque<IndexedTypeIdentifier> toFlush = new LinkedList<>();

      TriConsumer<Address, Void, Throwable> triConsumer = (a, v, t) -> {
         if (t != null) {
            throw new CacheException(t);
         }
      };
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
               indexingStrategy == IndexingExecutionMode.PRIMARY_OWNER, null);

         futures.add(executor.submitConsumer(indexWork, triConsumer));
      }
      CompletableFuture<Void> compositeFuture = CompletableFuture.allOf(futures.toArray(
            new CompletableFuture[0]));
      BiConsumer<Void, Throwable> consumer = (v, t) -> {
         for (IndexedTypeIdentifier type : toFlush) {
            indexUpdater.flush(type);
         }
      };
      if (asyncFlush) {
         compositeFuture = compositeFuture.whenCompleteAsync(consumer, Executors.newSingleThreadExecutor());
      } else {
         compositeFuture = compositeFuture.whenComplete(consumer);
      }
      return compositeFuture;
   }
}
