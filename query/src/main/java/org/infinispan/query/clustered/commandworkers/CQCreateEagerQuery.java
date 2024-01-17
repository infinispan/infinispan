package org.infinispan.query.clustered.commandworkers;

import static org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder.INFINISPAN_AGGREGATION_KEY_NAME;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.search.backend.lucene.search.query.LuceneSearchResult;
import org.hibernate.search.engine.search.aggregation.AggregationKey;
import org.hibernate.search.engine.search.query.SearchResultTotal;
import org.infinispan.query.clustered.NodeTopDocs;
import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;

/**
 * Returns the results of a node to create an eager distributed iterator.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
final class CQCreateEagerQuery extends CQWorker {

   @Override
   CompletionStage<QueryResponse> perform(BitSet segments) {
      SearchQueryBuilder query = queryDefinition.getSearchQueryBuilder();
      setFilter(segments);

      CompletionStage<NodeTopDocs> nodeTopDocs;
      if (query.aggregation() != null) {
         nodeTopDocs = collectAggregations(query);
      } else if (query.isEntityProjection()) {
         nodeTopDocs = collectKeys(query, queryDefinition.isScoreRequired());
      } else {
         nodeTopDocs = collectProjections(query);
      }

      return nodeTopDocs.thenApply(QueryResponse::new);
   }

   private LuceneSearchResult<?> fetchHits(SearchQueryBuilder query) {
      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

      LuceneSearchResult<?> result = query.build().fetch(queryDefinition.getMaxResults());

      if (queryStatistics.isEnabled())
         queryStatistics.localIndexedQueryExecuted(queryDefinition.getQueryString(), System.nanoTime() - start);

      return result;
   }

   public LuceneSearchResult<Object> fetchIds(SearchQueryBuilder query) {
      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

      LuceneSearchResult<Object> result = query.ids().fetch(queryDefinition.getMaxResults());

      if (queryStatistics.isEnabled()) {
         queryStatistics.localIndexedQueryExecuted(queryDefinition.getQueryString(), System.nanoTime() - start);
      }
      return result;
   }

   public LuceneSearchResult<List<Object>> fetchIdAndScore(SearchQueryBuilder query) {
      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

      LuceneSearchResult<List<Object>> result = query.idAndScore().fetch(queryDefinition.getMaxResults());

      if (queryStatistics.isEnabled()) {
         queryStatistics.localIndexedQueryExecuted(queryDefinition.getQueryString(), System.nanoTime() - start);
      }
      return result;
   }

   private CompletionStage<NodeTopDocs> collectKeys(SearchQueryBuilder query, boolean scoreRequired) {
      if (scoreRequired) {
         return blockingManager.supplyBlocking(() -> fetchIdAndScore(query), "CQCreateEagerQuery#collectKeys")
               .thenApply(queryResult -> {
                  SearchResultTotal total = queryResult.total();
                  int hitCount = Math.toIntExact(total.hitCountLowerBound());
                  boolean countIsExact = total.isHitCountExact();

                  Object[] keys = queryResult.hits().stream().map(objects -> objects.get(0))
                        .toArray(Object[]::new);
                  return new NodeTopDocs(cache.getRpcManager().getAddress(), queryResult.topDocs(), hitCount, countIsExact,
                        keys, null);
               });
      }

      return blockingManager.supplyBlocking(() -> fetchIds(query), "CQCreateEagerQuery#collectKeys")
            .thenApply(queryResult -> {
               SearchResultTotal total = queryResult.total();
               int hitCount = Math.toIntExact(total.hitCountLowerBound());
               boolean countIsExact = total.isHitCountExact();

               Object[] keys = queryResult.hits().stream()
                     .toArray(Object[]::new);
               return new NodeTopDocs(cache.getRpcManager().getAddress(), queryResult.topDocs(), hitCount, countIsExact,
                     keys, null);
            });
   }

   private CompletionStage<NodeTopDocs> collectProjections(SearchQueryBuilder query) {
      return blockingManager.supplyBlocking(() -> fetchHits(query), "CQCreateEagerQuery#collectProjections")
            .thenApply(queryResult -> {
               SearchResultTotal total = queryResult.total();
               int hitCount = Math.toIntExact(total.hitCountLowerBound());
               boolean countIsExact = total.isHitCountExact();

               List<?> hits = queryResult.hits();
               Object[] projections = hits.toArray(new Object[0]);
               return new NodeTopDocs(cache.getRpcManager().getAddress(), queryResult.topDocs(), hitCount, countIsExact,
                     null, projections);
            });
   }

   private CompletionStage<NodeTopDocs> collectAggregations(SearchQueryBuilder query) {
      boolean displayGroupFirst = query.aggregation().displayGroupFirst();

      return blockingManager.supplyBlocking(() -> fetchHits(query), "CQCreateEagerQuery#collectAggregations")
            .thenApply(queryResult -> {
               Map<?, Long> aggregation = queryResult.aggregation(AggregationKey.of(INFINISPAN_AGGREGATION_KEY_NAME));
               ArrayList<Object[]> hits = new ArrayList<>(aggregation.size());
               for (Map.Entry<?, Long> groupAggregation : aggregation.entrySet()) {
                  if (displayGroupFirst) {
                     hits.add(new Object[]{groupAggregation.getKey(), groupAggregation.getValue()});
                  } else {
                     hits.add(new Object[]{groupAggregation.getValue(), groupAggregation.getKey()});
                  }
               }

               Object[] projections = hits.toArray(new Object[0]);
               return new NodeTopDocs(cache.getRpcManager().getAddress(), queryResult.topDocs(), hits.size(), true,
                     null, projections);
            });
   }
}
