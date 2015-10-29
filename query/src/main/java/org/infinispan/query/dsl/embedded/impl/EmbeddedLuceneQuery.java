package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.query.CacheQuery;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;

import java.util.ArrayList;
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

   private final RowProcessor rowProcessor;

   /**
    * An Infinispan Cache query that wraps an actual Lucene query object. This is built lazily when the query is
    * executed first.
    */
   private CacheQuery cacheQuery;

   /**
    * The cached results, lazily evaluated.
    */
   private List<Object> results;

   EmbeddedLuceneQuery(QueryEngine queryEngine, QueryFactory queryFactory,
                       String jpaQuery, Map<String, Object> namedParameters,
                       String[] projection, RowProcessor rowProcessor,
                       long startOffset, int maxResults) {
      super(queryFactory, jpaQuery, namedParameters, projection, startOffset, maxResults);
      if (rowProcessor != null && (projection == null || projection.length == 0)) {
         throw new IllegalArgumentException("A RowProcessor can only be specified with projections");
      }
      this.queryEngine = queryEngine;
      this.rowProcessor = rowProcessor;
   }

   @Override
   public void resetQuery() {
      results = null;
      cacheQuery = null;
   }

   private CacheQuery createCacheQuery() {
      // query is created first time only
      if (cacheQuery == null) {
         cacheQuery = queryEngine.buildLuceneQuery(jpaQuery, namedParameters, startOffset, maxResults);
      }
      return cacheQuery;
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T> List<T> list() {
      if (results == null) {
         results = listInternal();
      }
      return (List<T>) results;
   }

   private List<Object> listInternal() {
      List<Object> list = createCacheQuery().list();
      if (rowProcessor != null) {
         results = new ArrayList<Object>(list.size());
         for (Object r : list) {
            Object[] inRow = (Object[]) r;
            Object[] outRow = rowProcessor.process(inRow);
            results.add(outRow);
         }
      } else {
         results = list;
      }
      return results;
   }

   @Override
   public int getResultSize() {
      return createCacheQuery().getResultSize();
   }

   @Override
   public String toString() {
      return "EmbeddedLuceneQuery{" +
            "jpaQuery=" + jpaQuery +
            ", namedParameters=" + namedParameters +
            '}';
   }
}
