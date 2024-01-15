package org.infinispan.query.dsl.embedded.impl;

import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.search.Sort;
import org.hibernate.search.backend.lucene.LuceneExtension;
import org.hibernate.search.backend.lucene.search.query.LuceneSearchQuery;
import org.hibernate.search.backend.lucene.search.query.dsl.LuceneSearchQueryOptionsStep;
import org.hibernate.search.engine.common.EntityReference;
import org.hibernate.search.engine.search.aggregation.AggregationKey;
import org.hibernate.search.engine.search.predicate.SearchPredicate;
import org.hibernate.search.engine.search.projection.SearchProjection;
import org.hibernate.search.engine.search.projection.dsl.SearchProjectionFactory;
import org.hibernate.search.engine.search.sort.SearchSort;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.session.SearchSession;

/**
 * Mutable builder for {@link LuceneSearchQuery}.
 *
 * @author Fabio Massimo Ercoli
 */
public final class SearchQueryBuilder {

   public static final String INFINISPAN_AGGREGATION_KEY_NAME = "InfinispanSingletonKey";

   private final SearchSession querySession;
   private final SearchScope<?> scope;
   private final SearchProjectionInfo projectionInfo;
   private final InfinispanAggregation<?> aggregation;
   private final SearchPredicate predicate;
   private final SearchSort sort;
   private final Integer knn;

   // target segment collection may mutate
   private Collection<String> routingKeys = Collections.emptySet();

   private int hitCountAccuracy;
   private Long timeout;
   private TimeUnit timeUnit;

   public SearchQueryBuilder(SearchSession querySession, SearchScope<?> scope, SearchProjectionInfo projectionInfo,
                             InfinispanAggregation<?> aggregation, SearchPredicate predicate, SearchSort sort,
                             int hitCountAccuracy, Integer knn) {
      this.querySession = querySession;
      this.scope = scope;
      this.projectionInfo = projectionInfo;
      this.aggregation = aggregation;
      this.predicate = predicate;
      this.sort = sort;
      this.hitCountAccuracy = hitCountAccuracy;
      this.knn = knn;
   }

   public LuceneSearchQuery<?> build() {
      return (aggregation == null) ? build(projectionInfo.getProjection()) :
            buildWithAggregation(projectionInfo.getProjection(), aggregation);
   }

   public LuceneSearchQuery<Object> ids() {
      return build(scope.projection().id().toProjection());
   }

   public LuceneSearchQuery<List<Object>> keyAndEntity() {
      SearchProjectionFactory<EntityReference, ?> projectionFactory = scope.projection();
      SearchProjection<?>[] searchProjections = new SearchProjection<?>[]{
            projectionFactory.entityReference().toProjection(),
            projectionFactory.entity().toProjection(),
            projectionFactory.score().toProjection()
      };
      return build((SearchProjection<List<Object>>) SearchProjectionInfo.composite(projectionFactory, searchProjections).getProjection());
   }

   public void routeOnSegments(BitSet segments) {
      routingKeys = segments.stream()
            .mapToObj(String::valueOf)
            .collect(Collectors.toList());
   }

   public void noRouting() {
      routingKeys = Collections.emptySet();
   }

   /**
    * Indicates if this query 'projects' just the entity and nothing else.
    */
   public boolean isEntityProjection() {
      return projectionInfo.isEntityProjection();
   }

   public Sort getLuceneSort() {
      return ids().luceneSort();
   }

   public Integer knn() {
      return knn;
   }

   public InfinispanAggregation<?> aggregation() {
      return aggregation;
   }

   private <T> LuceneSearchQuery<T> build(SearchProjection<T> searchProjection) {
      LuceneSearchQueryOptionsStep<T, ?> queryOptionsStep = querySession.search(scope)
            .extension(LuceneExtension.get())
            .select(searchProjection)
            .where(predicate)
            .sort(sort)
            .routing(routingKeys);

      if (timeout != null && timeUnit != null) {
         queryOptionsStep = queryOptionsStep.failAfter(timeout, timeUnit);
      }
      queryOptionsStep = queryOptionsStep.totalHitCountThreshold(hitCountAccuracy);

      return queryOptionsStep.toQuery();
   }

   private <T, A> LuceneSearchQuery<T> buildWithAggregation(SearchProjection<T> searchProjection,
                                                                   InfinispanAggregation<A> aggregation) {
      AggregationKey<Map<A, Long>> aggregationKey = AggregationKey.of(INFINISPAN_AGGREGATION_KEY_NAME);
      LuceneSearchQueryOptionsStep<T, ?> queryOptionsStep = querySession.search(scope)
            .extension(LuceneExtension.get())
            .select(searchProjection)
            .where(predicate)
            .aggregation(aggregationKey, aggregation.searchAggregation())
            .sort(sort)
            .routing(routingKeys);

      if (timeout != null && timeUnit != null) {
         queryOptionsStep = queryOptionsStep.failAfter(timeout, timeUnit);
      }
      // we don't need the hit count
      queryOptionsStep = queryOptionsStep.totalHitCountThreshold(0);

      return queryOptionsStep.toQuery();
   }

   public void hitCountAccuracy(int hitCountAccuracy) {
      this.hitCountAccuracy = hitCountAccuracy;
   }

   public void failAfter(long timeout, TimeUnit timeUnit) {
      this.timeout = timeout;
      this.timeUnit = timeUnit;
   }
}
