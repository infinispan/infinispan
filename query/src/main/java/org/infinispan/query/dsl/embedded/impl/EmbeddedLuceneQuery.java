package org.infinispan.query.dsl.embedded.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;


/**
 * A query implementation based on Lucene. No aggregations are present in the query.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
final class EmbeddedLuceneQuery<TypeMetadata> extends BaseQuery {

   private final QueryEngine<TypeMetadata> queryEngine;

   /**
    * Apply a postprocessing transformation to the query result (only when we have projections).
    */
   private final QueryEngine.RowProcessor rowProcessor;

   /**
    * The parsed Ickle query.
    */
   private final IckleParsingResult<TypeMetadata> parsingResult;

   /**
    * An Infinispan Cache query that wraps an actual Lucene query object. This is built lazily when the query is
    * executed first time.
    */
   private CacheQuery<?> cacheQuery;

   /**
    * The cached results, lazily evaluated.
    */
   private List<?> results;

   private final IndexedQueryMode queryMode;

   EmbeddedLuceneQuery(QueryEngine<TypeMetadata> queryEngine, QueryFactory queryFactory,
                       Map<String, Object> namedParameters, IckleParsingResult<TypeMetadata> parsingResult,
                       String[] projection, QueryEngine.RowProcessor rowProcessor,
                       long startOffset, int maxResults, IndexedQueryMode queryMode) {
      super(queryFactory, parsingResult.getQueryString(), namedParameters, projection, startOffset, maxResults);
      if (rowProcessor != null && (projection == null || projection.length == 0)) {
         throw new IllegalArgumentException("A RowProcessor can only be specified with projections");
      }
      this.queryEngine = queryEngine;
      this.queryMode = queryMode;
      this.rowProcessor = rowProcessor;
      this.parsingResult = parsingResult;
   }

   @Override
   public void resetQuery() {
      results = null;
      cacheQuery = null;
   }

   private CacheQuery<?> createCacheQuery() {
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
         List<?> list = createCacheQuery().list();
         results = rowProcessor == null ? list : ((List<Object[]>) list).stream().map(rowProcessor)
               .collect(Collectors.toCollection(() -> new ArrayList<>(list.size())));
      }
      return (List<T>) results;
   }

   @Override
   public int getResultSize() {
      //todo [anistor] optimize this by running a slightly modified query that performs just COUNT only, ignoring projections or sorting
      return createCacheQuery().getResultSize();
   }

   @Override
   public String toString() {
      return "EmbeddedLuceneQuery{queryString=" + queryString + ", namedParameters=" + namedParameters + '}';
   }
}
