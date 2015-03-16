package org.infinispan.query.dsl.embedded.impl;

import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.LuceneQuery;
import org.infinispan.query.dsl.impl.BaseQuery;

import java.util.List;

/**
 * A query implementation based on Lucene.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
final class EmbeddedLuceneQuery extends BaseQuery implements LuceneQuery {

   private final SearchManager sm;

   private final LuceneQueryParsingResult parsingResult;

   private final long startOffset;

   private final int maxResults;

   private CacheQuery cacheQuery = null;

   public EmbeddedLuceneQuery(QueryFactory queryFactory, String jpaQuery, SearchManager sm, LuceneQueryParsingResult parsingResult, long startOffset, int maxResults) {
      super(queryFactory, jpaQuery);
      this.sm = sm;
      this.parsingResult = parsingResult;
      this.startOffset = startOffset;
      this.maxResults = maxResults;
   }

   private CacheQuery getCacheQuery() {
      if (cacheQuery == null) {
         cacheQuery = sm.getQuery(parsingResult.getQuery(), parsingResult.getTargetEntity());
         if (parsingResult.getSort() != null) {
            cacheQuery = cacheQuery.sort(parsingResult.getSort());
         }
         if (parsingResult.getProjections() != null && !parsingResult.getProjections().isEmpty()) {
            cacheQuery = cacheQuery.projection(parsingResult.getProjections().toArray(new String[parsingResult.getProjections().size()]));
         }
         if (startOffset >= 0) {
            cacheQuery = cacheQuery.firstResult((int) startOffset);
         }
         if (maxResults >= 0) {
            cacheQuery = cacheQuery.maxResults(maxResults);
         }
      }
      return cacheQuery;
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T> List<T> list() {
      return (List<T>) getCacheQuery().list();
   }

   @Override
   public ResultIterator iterator(FetchOptions fetchOptions) {
      return getCacheQuery().iterator(fetchOptions);
   }

   @Override
   public ResultIterator iterator() {
      return getCacheQuery().iterator();
   }

   @Override
   public int getResultSize() {
      return getCacheQuery().getResultSize();
   }

   @Override
   public String toString() {
      return "EmbeddedLuceneQuery{" +
            "parsingResult=" + parsingResult +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            '}';
   }
}
