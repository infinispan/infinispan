package org.infinispan.query.dsl.embedded.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.core.impl.MappingIterator;
import org.infinispan.query.core.impl.QueryResultImpl;
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

   EmbeddedLuceneQuery(QueryEngine<TypeMetadata> queryEngine, QueryFactory queryFactory,
                       Map<String, Object> namedParameters, IckleParsingResult<TypeMetadata> parsingResult,
                       String[] projection, QueryEngine.RowProcessor rowProcessor,
                       long startOffset, int maxResults) {
      super(queryFactory, parsingResult.getQueryString(), namedParameters, projection, startOffset, maxResults);
      if (rowProcessor != null && (projection == null || projection.length == 0)) {
         throw new IllegalArgumentException("A RowProcessor can only be specified with projections");
      }
      this.queryEngine = queryEngine;
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
         cacheQuery = queryEngine.buildLuceneQuery(parsingResult, namedParameters, startOffset, maxResults);
         if (timeout > 0) {
            cacheQuery.timeout(timeout, TimeUnit.NANOSECONDS);
         }
      }
      return cacheQuery;
   }

   @Override
   public List<T> list() {
      return execute().list();
   }

   @Override
   public QueryResult<T> execute() {
      List<?> results = StreamSupport.stream(spliterator(), false)
            .map(i -> (projection == null) ? i : convertProjectionItem(i))
            .collect(Collectors.toList());

      return new QueryResultImpl<>(createCacheQuery().getResultSize(), (List<T>) results);
   }

   @Override
   public CloseableIterator<T> iterator() {
      IndexedQuery<T> cacheQuery = createCacheQuery();
      return new MappingIterator(cacheQuery.iterator(), i -> (projection == null) ? i : convertProjectionItem(i));
   }

   @Override
   public int getResultSize() {
      return Math.toIntExact(createCacheQuery().getResultSize());
   }

   @Override
   public String toString() {
      return "EmbeddedLuceneQuery{queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", timeout=" + timeout +
            +'}';
   }

   private Object[] convertProjectionItem(Object row) {
      Object[] array;
      if (row instanceof Object[]) {
         array = (Object[]) row;
      } else if (row instanceof List) {
         // Hibernate Search 6 uses list to wrap multiple item projection
         List<?> castedRow = (List<?>) row;
         array = castedRow.toArray(new Object[0]);
      } else {
         // Hibernate Search 6 does not wrap single item projection
         array = new Object[]{row};
      }

      return (rowProcessor == null) ? array : rowProcessor.apply(array);
   }
}
