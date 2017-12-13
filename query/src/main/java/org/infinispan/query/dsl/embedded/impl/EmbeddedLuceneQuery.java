package org.infinispan.query.dsl.embedded.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;


/**
 * A query implementation based on Lucene.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
final class EmbeddedLuceneQuery<TypeMetadata> extends BaseQuery {

   private final QueryEngine<TypeMetadata> queryEngine;

   private final ResultProcessor resultProcessor;

   private final IckleParsingResult<TypeMetadata> parsingResult;

   /**
    * An Infinispan Cache query that wraps an actual Lucene query object. This is built lazily when the query is
    * executed first.
    */
   private CacheQuery<Object> cacheQuery;

   /**
    * The cached results, lazily evaluated.
    */
   private List<Object> results;
   private final IndexedQueryMode queryMode;

   EmbeddedLuceneQuery(QueryEngine<TypeMetadata> queryEngine, QueryFactory queryFactory,
                       Map<String, Object> namedParameters, IckleParsingResult<TypeMetadata> parsingResult,
                       String[] projection, ResultProcessor resultProcessor,
                       long startOffset, int maxResults, IndexedQueryMode queryMode) {
      super(queryFactory, parsingResult.getQueryString(), namedParameters, projection, startOffset, maxResults);
      if (resultProcessor instanceof RowProcessor && (projection == null || projection.length == 0)) {
         throw new IllegalArgumentException("A RowProcessor can only be specified with projections");
      }
      this.queryEngine = queryEngine;
      this.queryMode = queryMode;
      this.resultProcessor = resultProcessor;
      this.parsingResult = parsingResult;
   }

   @Override
   public void resetQuery() {
      results = null;
      cacheQuery = null;
   }

   private CacheQuery<Object> createCacheQuery() {
      // query is created first time only
      if (cacheQuery == null) {
         validateNamedParameters();
         cacheQuery = queryEngine.buildLuceneQuery(parsingResult, namedParameters, startOffset, maxResults, queryMode);
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
         list.forEach(r -> results.add(resultProcessor.process(r)));
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
            "queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            '}';
   }
}
