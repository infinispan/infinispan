package org.infinispan.query.dsl.embedded.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
   private IndexedQuery<T> indexedQuery;

   /**
    * An Infinispan Cache query to use for {@link #list()} and {@link #iterator()},
    * that wraps an actual Lucene query object.
    * This is built lazily when {@link #list()} or {@link #iterator()} is executed first time.
    * In general more efficient than {@link #indexedQuery}, since we don't need to return the hit count.
    */
   private IndexedQuery<T> indexedListQuery;

   EmbeddedLuceneQuery(QueryEngine<TypeMetadata> queryEngine, QueryFactory queryFactory,
                       Map<String, Object> namedParameters, IckleParsingResult<TypeMetadata> parsingResult,
                       String[] projection, QueryEngine.RowProcessor rowProcessor,
                       long startOffset, int maxResults, boolean local) {
      super(queryFactory, parsingResult.getQueryString(), namedParameters, projection, startOffset, maxResults, local);
      if (rowProcessor != null && (projection == null || projection.length == 0)) {
         throw new IllegalArgumentException("A RowProcessor can only be specified with projections");
      }
      this.queryEngine = queryEngine;
      this.rowProcessor = rowProcessor;
      this.parsingResult = parsingResult;
   }

   @Override
   public void resetQuery() {
      indexedQuery = null;
      indexedListQuery = null;
   }

   @Override
   public List<T> list() {
      IndexedQuery<?> indexedQuery = getOrCreateIndexedQuery(false);
      QueryResult<?> result = indexedQuery.execute();
      List<Object> collect = result.list().stream().map(this::convertResult).collect(Collectors.toList());
      return (List<T>) collect;
   }

   @Override
   public QueryResult<T> execute() {
      IndexedQuery<?> indexedQuery = getOrCreateIndexedQuery(true);
      QueryResult<?> result = indexedQuery.execute();
      List<Object> collect = result.list().stream().map(this::convertResult).collect(Collectors.toList());
      return new QueryResultImpl<>(result.count(), (List<T>) collect);
   }

   @Override
   public int executeStatement() {
      IndexedQuery<T> indexedQuery = getOrCreateIndexedQuery(false);
      return indexedQuery.executeStatement();
   }

   @Override
   public CloseableIterator<T> iterator() {
      return new MappingIterator(getOrCreateIndexedQuery(true).iterator(), this::convertResult);
   }

   @Override
   public <K> CloseableIterator<Map.Entry<K, T>> entryIterator() {
      return new MappingIterator(getOrCreateIndexedQuery(true).entryIterator(), null);
   }

   @Override
   public int getResultSize() {
      return getOrCreateIndexedQuery(true).getResultSize();
   }

   @Override
   public String toString() {
      return "EmbeddedLuceneQuery{queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", timeout=" + timeout +
            '}';
   }

   private IndexedQuery<T> getOrCreateIndexedQuery(boolean withHitCount) {
      if (withHitCount) {
         if (indexedQuery == null) {
            indexedQuery = createQuery(true);
         }
         return indexedQuery;
      } else {
         if (indexedListQuery == null) {
            indexedListQuery = createQuery(false);
         }
         return indexedListQuery;
      }
   }

   private IndexedQuery<T> createQuery(boolean withHitCount) {
      validateNamedParameters();
      IndexedQuery<T> result = queryEngine.buildLuceneQuery(parsingResult, namedParameters, startOffset, maxResults, isLocal());

      if (withHitCount) {
         if (hitCountAccuracy != null) {
            result = result.hitCountAccuracy(hitCountAccuracy);
         }
      } else {
         result.hitCountAccuracy(1); // lower the hit count accuracy
      }

      if (timeout > 0) {
         result.timeout(timeout, TimeUnit.NANOSECONDS);
      }

      return result;
   }

   private Object convertResult(Object result) {
      if (projection == null) return result;

      Object[] array;
      if (result instanceof Object[]) {
         array = (Object[]) result;
      } else if (result instanceof List) {
         // Hibernate Search 6 uses list to wrap multiple item projection
         List<?> castedRow = (List<?>) result;
         array = castedRow.toArray(new Object[0]);
      } else {
         // Hibernate Search 6 does not wrap single item projection
         array = new Object[]{result};
      }

      return rowProcessor == null ? array : rowProcessor.apply(array);
   }
}
