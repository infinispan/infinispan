package org.infinispan.query.impl.massindex;

import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.concurrent.Futures;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.impl.massindex.MassIndexStrategy.CleanExecutionMode;
import org.infinispan.query.impl.massindex.MassIndexStrategy.FlushExecutionMode;
import org.infinispan.query.impl.massindex.MassIndexStrategy.IndexingExecutionMode;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.query.impl.massindex.MassIndexStrategyFactory.calculateStrategy;

/**
 * @author gustavonalle
 * @since 7.1
 */
public class DistributedExecutorMassIndexer implements MassIndexer {

   private static final Log LOG = LogFactory.getLog(DistributedExecutorMassIndexer.class, Log.class);

   private final AdvancedCache cache;
   private final SearchIntegrator searchIntegrator;
   private final IndexUpdater indexUpdater;
   private final DistributedExecutorService executor;

   public DistributedExecutorMassIndexer(AdvancedCache cache, SearchIntegrator searchIntegrator) {
      this.cache = cache;
      this.searchIntegrator = searchIntegrator;
      this.indexUpdater = new IndexUpdater(cache);
      this.executor = new DefaultExecutorService(cache);
   }

   @Override
   @SuppressWarnings("unchecked")
   public void start() {
      ExecutionResult<Void> executionResult = executeInternal();
      executionResult.waitForAll();
      executionResult.flushIfNeed();
   }

   @Override
   public NotifyingFuture<Void> startAsync() {
      final ExecutionResult<Void> executionResult = executeInternal();
      NotifyingFuture<?> combined = Futures.combine(executionResult.futures);
      return Futures.andThen(combined, new Runnable() {
         @Override
         public void run() {
            executionResult.flushIfNeed();
         }
      }, Executors.newSingleThreadExecutor());
   }

   private ExecutionResult<Void> executeInternal() {
      List<NotifyingFuture<Void>> futures = new ArrayList<>();
      Deque<Class<?>> toFlush = new LinkedList<>();

      for (Class<?> indexedType : searchIntegrator.getIndexedTypes()) {
         EntityIndexBinding indexBinding = searchIntegrator.getIndexBinding(indexedType);
         MassIndexStrategy strategy = calculateStrategy(indexBinding, cache.getAdvancedCache().getCacheConfiguration());
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
                 new IndexWorker(indexedType, workerFlush, workerClean, indexingStrategy == IndexingExecutionMode.PRIMARY_OWNER);

         DistributedTask<Void> task = executor
                 .createDistributedTaskBuilder(indexWork)
                 .timeout(0, TimeUnit.NANOSECONDS)
                 .build();

         futures.addAll((List) executor.submitEverywhere(task));

      }
      return new ExecutionResult<>(futures, toFlush);

   }

   private class ExecutionResult<T> {
      final List<NotifyingFuture<T>> futures;
      final Queue<Class<?>> toFlush;

      public ExecutionResult(List<NotifyingFuture<T>> futures, Queue<Class<?>> toFlush) {
         this.futures = futures;
         this.toFlush = toFlush;
      }

      void flushIfNeed() {
         for (Class<?> type : toFlush) {
            indexUpdater.flush(type);
         }
      }

      private void waitForAll() {
         for (Future f : futures) {
            try {
               f.get();
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
               LOG.errorExecutingMassIndexer(e);
            }
         }
      }
   }

   private boolean isShared(IndexManager indexManager) {
      return indexManager instanceof InfinispanIndexManager;
   }

}
