package org.infinispan.query.stats.impl;

import org.hibernate.search.backend.lucene.index.LuceneIndexManager;
import org.hibernate.search.engine.search.predicate.dsl.SearchPredicateFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.query.Indexer;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.core.stats.IndexStatisticsSnapshot;
import org.infinispan.query.core.stats.impl.IndexStatisticsSnapshotImpl;
import org.infinispan.search.mapper.mapping.SearchIndexedEntity;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.session.SearchSession;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletionStages;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * A {@link IndexStatistics} for an indexed Cache.
 * @since 12.0
 */
@Scope(Scopes.NAMED_CACHE)
public class LocalIndexStatistics implements IndexStatistics {
   @Inject
   SearchMapping searchMapping;

   @Inject
   BlockingManager blockingManager;

    @Inject
   Indexer indexer;

   @Override
   public Set<String> indexedEntities() {
      return searchMapping.allIndexedEntityNames();
   }

   @Override
   public CompletionStage<Map<String, IndexInfo>> computeIndexInfos() {
      Map<String, CompletionStage<IndexInfo>> infoStages = new HashMap<>();
      AggregateCompletionStage<Map<String, IndexInfo>> aggregateCompletionStage =
            CompletionStages.aggregateCompletionStage(new HashMap<>());
      for (SearchIndexedEntity entity : searchMapping.allIndexedEntities()) {
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
      SearchSession session = searchMapping.getMappingSession();
      SearchScope<?> scope = session.scope(indexedEntity.javaClass(), indexedEntity.name());
      CompletionStage<Long> countStage = blockingManager.supplyBlocking(
            () -> session.search(scope).where(SearchPredicateFactory::matchAll).fetchTotalHitCount(), this);
      CompletionStage<Long> sizeStage = indexedEntity.indexManager().unwrap(LuceneIndexManager.class)
            .computeSizeInBytesAsync();
      return countStage.thenCombine(sizeStage, IndexInfo::new);
   }

   @Override
   public boolean reindexing() {
      return indexer.isRunning();
   }

    @Override
    public CompletionStage<IndexStatisticsSnapshot> computeSnapshot() {
        return computeIndexInfos().thenApply(infos -> new IndexStatisticsSnapshotImpl(infos, reindexing()));
    }
}
