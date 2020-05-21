package org.infinispan.query.dsl.embedded.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.core.impl.QueryResultImpl;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.impl.IndexedQuery;


/**
 * A query implementation based on Lucene. No aggregations are present in the query.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
final class EmbeddedLuceneQuery<TypeMetadata, T> extends BaseQuery<T> {

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
   private IndexedQuery<T> cacheQuery;

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
      cacheQuery = null;
   }

   private IndexedQuery<T> createCacheQuery() {
      // query is created first time only
      if (cacheQuery == null) {
         validateNamedParameters();
         cacheQuery = queryEngine.buildLuceneQuery(parsingResult, namedParameters, startOffset, maxResults, queryMode);
      }
      return cacheQuery;
   }

   @Override
   @SuppressWarnings("unchecked")
   public List<T> list() {
      return execute().list();
   }

   @Override
   public QueryResult<T> execute() {
      IndexedQuery<T> cacheQuery = createCacheQuery();
      List<T> list = cacheQuery.list();
      int hits = cacheQuery.getResultSize();
      List<?> results = rowProcessor == null ? list : ((List<Object[]>) list).stream().map(rowProcessor)
            .collect(Collectors.toCollection(() -> new ArrayList<>(list.size())));

      return new QueryResultImpl<>(OptionalLong.of(hits), (List<T>) results);
   }

   @Override
   public CloseableIterator<T> iterator() {
      return createCacheQuery().iterator();
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
