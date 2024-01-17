package org.infinispan.query.core.impl;

import java.util.BitSet;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.query.EntityEntry;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.objectfilter.impl.syntax.parser.projection.ScorePropertyPath;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

public class MetadataHybridQuery<T, S> extends HybridQuery<T, S> {

   private final BitSet scoreProjections;

   public MetadataHybridQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String queryString,
                              IckleParsingResult.StatementType statementType, Map<String, Object> namedParameters,
                              ObjectFilter objectFilter, long startOffset, int maxResults, Query<?> baseQuery,
                              LocalQueryStatistics queryStatistics, boolean local) {
      super(queryFactory, cache, queryString, statementType, namedParameters, objectFilter, startOffset, maxResults, baseQuery, queryStatistics, local);

      scoreProjections = new BitSet();
      String[] projection = objectFilter.getProjection();
      if (projection == null) {
         return;
      }

      for (int i = 0; i < projection.length; i++) {
         if (ScorePropertyPath.SCORE_PROPERTY_NAME.equals(projection[i])) {
            scoreProjections.set(i);
         }
      }
   }

   @Override
   protected CloseableIterator<ObjectFilter.FilterResult> getInternalIterator() {
      CloseableIterator<EntityEntry<Object, S>> iterator = baseQuery
            .startOffset(0).maxResults(Integer.MAX_VALUE).local(local)
            .scoreRequired(scoreProjections.cardinality() > 0).entryIterator();
      return new MappingEntryIterator<>(iterator, this::filter);
   }

   private ObjectFilter.FilterResult filter(EntityEntry entry) {
      ObjectFilter.FilterResult filter = objectFilter.filter(entry.key(), entry.value());
      String[] projection = objectFilter.getProjection();
      if (projection == null) {
         return filter;
      }

      scoreProjections.stream().forEach(i -> filter.getProjection()[i] = entry.score());
      return filter;
   }
}
