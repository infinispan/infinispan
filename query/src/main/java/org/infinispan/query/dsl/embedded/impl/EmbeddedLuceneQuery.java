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

   private final ResultProcessor resultProcessor;

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
                       String[] projection, ResultProcessor resultProcessor,
                       long startOffset, int maxResults) {
      super(queryFactory, jpaQuery, namedParameters, projection, startOffset, maxResults);
      if (resultProcessor instanceof RowProcessor && (projection == null || projection.length == 0)) {
         throw new IllegalArgumentException("A RowProcessor can only be specified with projections");
      }
      this.queryEngine = queryEngine;
      this.resultProcessor = resultProcessor;
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
      if (resultProcessor != null) {
         results = new ArrayList<>(list.size());
         for (Object r : list) {
            Object o = resultProcessor.process(r);
            results.add(o);
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
