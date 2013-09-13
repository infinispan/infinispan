package org.infinispan.query.dsl.embedded.impl;

import org.apache.lucene.search.Sort;
import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.embedded.LuceneQuery;

import java.util.List;

/**
 * A query implementation based on Lucene.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
class EmbeddedLuceneQuery implements LuceneQuery {

   private final SearchManager sm;

   private final LuceneQueryParsingResult parsingResult;

   private final Sort sort;

   private final long startOffset;

   private final int maxResults;

   private CacheQuery cacheQuery = null;

   public EmbeddedLuceneQuery(SearchManager sm, LuceneQueryParsingResult parsingResult, Sort sort, long startOffset, int maxResults) {
      this.sm = sm;
      this.parsingResult = parsingResult;
      this.sort = sort;
      this.startOffset = startOffset;
      this.maxResults = maxResults;
   }

   private CacheQuery getCacheQuery() {
      if (cacheQuery == null) {
         cacheQuery = sm.getQuery(parsingResult.getQuery(), parsingResult.getTargetEntity());
         if (sort != null) {
            cacheQuery = cacheQuery.sort(sort);
         }
         if (parsingResult.getProjections() != null && !parsingResult.getProjections().isEmpty()) {
            cacheQuery = cacheQuery.projection(parsingResult.getProjections().toArray(new String[parsingResult.getProjections().size()]));
         }
         if (startOffset > 0) {
            cacheQuery = cacheQuery.firstResult((int) startOffset);
         }
         if (maxResults > 0) {
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
            ", sort=" + sort +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            '}';
   }
}
