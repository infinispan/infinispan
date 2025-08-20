package org.infinispan.query.objectfilter.test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.query.BaseQuery;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
final class FilterQueryFactory implements QueryFactory {

   FilterQueryFactory() {
   }

   @Override
   public <T> Query<T> create(String queryString) {
      return new FilterQuery<>(queryString, null, null, -1, -1);
   }

   private static final class FilterQuery<T> extends BaseQuery<T> {

      FilterQuery(String queryString, Map<String, Object> namedParameters, String[] projection, long startOffset, int maxResults) {
         super(queryString, namedParameters, projection, startOffset, maxResults, false);
      }

      @Override
      public CloseableIterator<T> iterator() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void resetQuery() {
      }

      @Override
      public QueryResult<T> execute() {
         throw new UnsupportedOperationException();
      }

      @Override
      public CompletionStage<org.infinispan.commons.api.query.QueryResult<T>> executeAsync() {
         throw new UnsupportedOperationException();
      }

      @Override
      public int executeStatement() {
         throw new UnsupportedOperationException();
      }

      @Override
      public CompletionStage<Integer> executeStatementAsync() {
         throw new UnsupportedOperationException();
      }

      // TODO [anistor] need to rethink the dsl Query/QueryBuilder interfaces to accommodate the filtering scenario ...
      @Override
      public List<T> list() {
         throw new UnsupportedOperationException();
      }

      @Override
      public int getResultSize() {
         throw new UnsupportedOperationException();
      }
   }
}
