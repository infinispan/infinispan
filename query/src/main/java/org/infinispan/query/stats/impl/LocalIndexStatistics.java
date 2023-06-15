package org.infinispan.query.stats.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.search.backend.lucene.index.LuceneIndexManager;
import org.hibernate.search.engine.backend.work.execution.OperationSubmitter;
import org.hibernate.search.engine.common.execution.spi.SimpleScheduledExecutor;
import org.hibernate.search.engine.search.predicate.dsl.SearchPredicateFactory;
import org.hibernate.search.util.common.SearchException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.query.Indexer;
import org.infinispan.query.concurrent.InfinispanIndexingExecutorProvider;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.core.stats.IndexStatisticsSnapshot;
import org.infinispan.query.core.stats.impl.IndexStatisticsSnapshotImpl;
import org.infinispan.query.logging.Log;
import org.infinispan.search.mapper.mapping.SearchIndexedEntity;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.session.SearchSession;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link IndexStatistics} for an indexed Cache.
 *
 * @since 12.0
 */
@Scope(Scopes.NAMED_CACHE)
public class LocalIndexStatistics implements IndexStatistics {

   private static final Log log = LogFactory.getLog(LocalIndexStatistics.class, Log.class);

   @Inject
   SearchMapping searchMapping;

   @Inject
   BlockingManager blockingManager;

   @Inject
   Indexer indexer;

   private SimpleScheduledExecutor offloadingExecutor;

   @Start
   void start() {
      offloadingExecutor = InfinispanIndexingExecutorProvider.writeExecutor(blockingManager);
   }

   @Override
   public Set<String> indexedEntities() {
      return searchMapping.allIndexedEntityNames();
   }

   @Override
   public CompletionStage<Map<String, IndexInfo>> computeIndexInfos() {
      Map<String, CompletionStage<IndexInfo>> infoStages = new HashMap<>();
      AggregateCompletionStage<Map<String, IndexInfo>> aggregateCompletionStage =
            CompletionStages.aggregateCompletionStage(new HashMap<>());
      for (SearchIndexedEntity entity : searchMapping.indexedEntitiesForStatistics()) {
         CompletionStage<IndexInfo> stage = indexInfos(entity);
         infoStages.put(entity.name(), stage);
         aggregateCompletionStage.dependsOn(stage);
      }
      return aggregateCompletionStage.freeze()
            .thenApply(map -> {
               // When we get here, all stages have completed successfully, so the join is safe.
               infoStages.forEach((name, stage) -> map.put(name, CompletionStages.join(stage)));
               return map;
            });
   }

   private CompletionStage<IndexInfo> indexInfos(SearchIndexedEntity indexedEntity) {
      CompletionStage<Long> countStage = blockingManager.supplyBlocking(
            () -> {
               SearchSession session = searchMapping.getMappingSession();
               SearchScope<?> scope = session.scope(indexedEntity.javaClass(), indexedEntity.name());
               long hitCount = -1;
               try {
                  hitCount = session.search(scope).where(SearchPredicateFactory::matchAll).fetchTotalHitCount();
               } catch (Throwable throwable) {
                  log.concurrentReindexingOnGetStatistics(throwable);
               }
               return hitCount;
               }, this);

      return countStage
            .thenCompose(count -> {
               CompletionStage<Long> sizeStage;
               if (count == -1L || reindexing()) {
                  sizeStage = CompletableFuture.completedFuture(-1L);
               } else {
                  try {
                     sizeStage = indexedEntity.indexManager().unwrap(LuceneIndexManager.class)
                           .computeSizeInBytesAsync(OperationSubmitter.offloading(task -> offloadingExecutor.submit(task)))
                           .exceptionally(throwable -> {
                              log.concurrentReindexingOnGetStatistics(throwable);
                              return -1L;
                           });
                  } catch (SearchException exception) {
                     if (exception.getMessage().contains("HSEARCH000563")) {
                        // in this case the engine orchestrator is stopped (not allowing to submit more tasks),
                        // it means that the engine is stopping,
                        // it means we cannot compute side at the moment
                        sizeStage = CompletableFuture.completedFuture(-1L);
                     } else {
                        // in the all other cases we get an unexpected error from the search engine
                        throw exception;
                     }
                  }
               }
               return sizeStage.thenApply(size -> new IndexInfo(count, size));
            });
   }

   @Override
   public boolean reindexing() {
      return indexer.isRunning();
   }

   @Override
   public int genericIndexingFailures() {
      return searchMapping.genericIndexingFailures();
   }

   @Override
   public int entityIndexingFailures() {
      return searchMapping.entityIndexingFailures();
   }

   @Override
   public CompletionStage<IndexStatisticsSnapshot> computeSnapshot() {
      return computeIndexInfos().thenApply(infos -> new IndexStatisticsSnapshotImpl(infos, reindexing(), genericIndexingFailures(), entityIndexingFailures()));
   }
}
