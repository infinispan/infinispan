package org.infinispan.query.stats.impl;

import java.util.Map;
import java.util.stream.Collectors;

import org.hibernate.search.backend.lucene.index.LuceneIndexManager;
import org.hibernate.search.engine.search.predicate.dsl.SearchPredicateFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.query.Indexer;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.core.stats.impl.IndexStatisticSnapshot;
import org.infinispan.search.mapper.mapping.SearchIndexedEntity;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.session.SearchSession;

/**
 * A {@link IndexStatistics} for an indexed Cache.
 * @since 12.0
 */
@Scope(Scopes.NAMED_CACHE)
public class LocalIndexStatistics implements IndexStatistics {
   @Inject
   SearchMapping searchMapping;

   @Inject
   Indexer indexer;

   @Override
   public Map<String, IndexInfo> indexInfos() {
      SearchSession session = searchMapping.getMappingSession();
      return searchMapping.allIndexedEntities().stream()
            .collect(Collectors.toMap(SearchIndexedEntity::name, e -> {
               Class<?> javaClass = e.javaClass();
               SearchScope<?> scope = session.scope(javaClass, e.name());
               long count = session.search(scope).where(SearchPredicateFactory::matchAll).fetchTotalHitCount();
               long size = e.indexManager().unwrap(LuceneIndexManager.class).computeSizeInBytes();
               return new IndexInfo(count, size);
            }));
   }

   @Override
   public IndexStatistics merge(IndexStatistics other) {
      return this;
   }

   @Override
   public IndexStatistics getSnapshot() {
      return new IndexStatisticSnapshot(indexInfos());
   }

   @Override
   public boolean reindexing() {
      return indexer.isRunning();
   }
}
