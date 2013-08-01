package org.infinispan.query.dsl.impl;

import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.Query;

import java.util.List;

/**
 * A query implementation based on Lucene.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
class LuceneQuery implements Query {

   private final SearchManager sm;

   private final LuceneQueryParsingResult parsingResult;

   private CacheQuery cacheQuery = null;

   public LuceneQuery(SearchManager sm, LuceneQueryParsingResult parsingResult) {
      this.sm = sm;
      this.parsingResult = parsingResult;
   }

   private CacheQuery getCacheQuery() {
      if (cacheQuery == null) {
         cacheQuery = sm.getQuery(parsingResult.getQuery(), parsingResult.getTargetEntity());
      }
      return cacheQuery;
   }

   @Override
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

   public String toString() {
      return "Lucene Query: " + parsingResult;
   }
}
