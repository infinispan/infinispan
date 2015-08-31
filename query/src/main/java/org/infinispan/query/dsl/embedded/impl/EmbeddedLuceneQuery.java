package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.query.CacheQuery;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;

import java.util.List;
import java.util.Map;


/**
 * A query implementation based on Lucene.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
final class EmbeddedLuceneQuery extends BaseQuery {

   private final QueryEngine queryEngine;

   /**
    * An Infinispan Cache query that wraps an actual Lucene query object. This is built lazily when the query is
    * executed first.
    */
   private CacheQuery cacheQuery;

   EmbeddedLuceneQuery(QueryEngine queryEngine, QueryFactory queryFactory, String jpaQuery, Map<String, Object> namedParameters, String[] projection,
                       long startOffset, int maxResults) {
      super(queryFactory, jpaQuery, namedParameters, projection, startOffset, maxResults);
      this.queryEngine = queryEngine;
   }

   @Override
   public void resetQuery() {
      cacheQuery = null;
   }

   private CacheQuery createQuery() {
      // query is created first time only
      if (cacheQuery == null) {
         cacheQuery = queryEngine.buildLuceneQuery(jpaQuery, namedParameters, startOffset, maxResults);
      }
      return cacheQuery;
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T> List<T> list() {
      return (List<T>) createQuery().list();
   }

   @Override
   public int getResultSize() {
      return createQuery().getResultSize();
   }

   @Override
   public String toString() {
      return "EmbeddedLuceneQuery{" +
            "jpaQuery=" + jpaQuery +
            ", namedParameters=" + namedParameters +
            '}';
   }
}
