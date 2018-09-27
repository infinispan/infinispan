package org.infinispan.query.impl.massindex;

import static org.infinispan.query.impl.massindex.MassIndexStrategyFactory.calculateStrategy;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.impl.massindex.MassIndexStrategy.CleanExecutionMode;
import org.infinispan.query.impl.massindex.MassIndexStrategy.FlushExecutionMode;
import org.infinispan.query.impl.massindex.MassIndexStrategy.IndexingExecutionMode;
import org.infinispan.query.logging.Log;
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
   private final DistributedExecutorService executor;

   public DistributedExecutorMassIndexer(AdvancedCache cache, SearchIntegrator searchIntegrator,
                                         KeyTransformationHandler keyTransformationHandler, TimeService timeService) {
      this.cache = cache;
      this.searchIntegrator = searchIntegrator;
      this.indexUpdater = new IndexUpdater(searchIntegrator, keyTransformationHandler, timeService);
      this.executor = new DefaultExecutorService(cache);
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
      List<CompletableFuture<Void>> futures = new ArrayList<>();
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
      if (everywhereSet.size() > 0) {
         IndexWorker indexWorkEverywhere =
               new IndexWorker(null, false, false, false, everywhereSet);

         DistributedTask<Void> taskEverywhere = executor
               .createDistributedTaskBuilder(indexWorkEverywhere)
               .timeout(0, TimeUnit.NANOSECONDS)
               .build();

         List<CompletableFuture<Void>> futureList = executor.submitEverywhere(taskEverywhere);
         addFutureListToFutures(futures, futureList);
      }
      if (primeownerSet.size() > 0) {
         IndexWorker indexWorkEverywhere =
               new IndexWorker(null, false, false, false, null);

         DistributedTask<Void> taskEverywhere = executor
               .createDistributedTaskBuilder(indexWorkEverywhere)
               .timeout(0, TimeUnit.NANOSECONDS)
               .build();

         List<CompletableFuture<Void>> futureList = executor.submitEverywhere(taskEverywhere, primeownerSet.toArray());
         addFutureListToFutures(futures, futureList);
      }
      return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
   }

   private CompletableFuture<Void> executeInternal(boolean asyncFlush) {
      List<CompletableFuture<Void>> futures = new ArrayList<>();
      Deque<IndexedTypeIdentifier> toFlush = new LinkedList<>();

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
         IndexWorker indexWork =
               new IndexWorker(indexedType, workerFlush, workerClean, indexingStrategy == IndexingExecutionMode.PRIMARY_OWNER, null);

         DistributedTask<Void> task = executor
               .createDistributedTaskBuilder(indexWork)
               .timeout(0, TimeUnit.NANOSECONDS)
               .build();

         List<CompletableFuture<Void>> futureList = executor.submitEverywhere(task);
         addFutureListToFutures(futures, futureList);
      }
      CompletableFuture<Void> compositeFuture = CompletableFuture.allOf(futures.toArray(
            new CompletableFuture[futures.size()]));
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
