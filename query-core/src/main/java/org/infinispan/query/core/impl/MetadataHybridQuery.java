package org.infinispan.query.core.impl;

import java.util.BitSet;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.query.ClosableIteratorWithCount;
import org.infinispan.commons.api.query.EntityEntry;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.objectfilter.impl.syntax.parser.projection.ScorePropertyPath;
import org.infinispan.objectfilter.impl.syntax.parser.projection.VersionPropertyPath;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

public class MetadataHybridQuery<T, S> extends HybridQuery<T, S> {

   private final BitSet scoreProjections;
   private final boolean versionProjection;

   public MetadataHybridQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String queryString,
                              IckleParsingResult.StatementType statementType, Map<String, Object> namedParameters,
                              ObjectFilter objectFilter, long startOffset, int maxResults, Query<?> baseQuery,
                              LocalQueryStatistics queryStatistics, boolean local, boolean allSortFieldsAreStored) {
      super(queryFactory, cache, queryString, statementType, namedParameters, objectFilter, startOffset, maxResults,
            baseQuery, queryStatistics, local, allSortFieldsAreStored);

      scoreProjections = new BitSet();
      String[] projection = objectFilter.getProjection();
      if (projection == null) {
         versionProjection = false;
         return;
      }

      boolean needVersion = false;
      for (int i = 0; i < projection.length; i++) {
         if (ScorePropertyPath.SCORE_PROPERTY_NAME.equals(projection[i])) {
            scoreProjections.set(i);
         }
         if (VersionPropertyPath.VERSION_PROPERTY_NAME.equals(projection[i])) {
            needVersion = true;
         }
      }
      versionProjection = needVersion;
   }

   @Override
   protected ClosableIteratorWithCount<ObjectFilter.FilterResult> getInternalIterator() {
      ClosableIteratorWithCount<EntityEntry<Object, S>> iterator = baseQuery
            .startOffset(0).maxResults(hybridMaxResult()).local(local)
            .scoreRequired(scoreProjections.cardinality() > 0).entryIterator(versionProjection);
      return new MappingEntryIterator<>(iterator, this::filter, iterator.count());
   }

   private ObjectFilter.FilterResult filter(EntityEntry<Object, S> entry) {
      S value = entry.value();
      if (value == null) {
         // A value can be null in case of expired entity,
         // there is a moment in which the index state and the data state are not aligned,
         // in this case it is safe to return a null value result:
         return null;
      }

      ObjectFilter.FilterResult filter = objectFilter.filter(entry.key(), value, entry.metadata());
      String[] projection = objectFilter.getProjection();
      if (projection == null) {
         return filter;
      }

      scoreProjections.stream().forEach(i -> filter.getProjection()[i] = entry.score());
      return filter;
   }
}
