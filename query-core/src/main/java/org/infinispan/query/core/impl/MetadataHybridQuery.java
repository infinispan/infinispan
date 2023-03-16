package org.infinispan.query.core.impl;

import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

public class MetadataHybridQuery<T, S> extends HybridQuery<T, S> {

   public MetadataHybridQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String queryString,
                              IckleParsingResult.StatementType statementType, Map<String, Object> namedParameters,
                              ObjectFilter objectFilter, long startOffset, int maxResults, Query<?> baseQuery,
                              LocalQueryStatistics queryStatistics, boolean local) {
      super(queryFactory, cache, queryString, statementType, namedParameters, objectFilter, startOffset, maxResults, baseQuery, queryStatistics, local);
   }

   @Override
   protected CloseableIterator<ObjectFilter.FilterResult> getInternalIterator() {
      CloseableIterator<Map.Entry<Object, S>> iterator = baseQuery
            .startOffset(0).maxResults(Integer.MAX_VALUE).local(local).entryIterator();
      return new MappingEntryIterator<>(iterator, objectFilter::filter);
   }
}
