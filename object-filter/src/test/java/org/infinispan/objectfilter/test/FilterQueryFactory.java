package org.infinispan.objectfilter.test;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;
import org.infinispan.query.dsl.impl.BaseQueryFactory;
import org.infinispan.query.dsl.impl.QueryStringCreator;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
final class FilterQueryFactory extends BaseQueryFactory {

   private final SerializationContext serializationContext;

   FilterQueryFactory(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   FilterQueryFactory() {
      this(null);
   }

   @Override
   public <T> Query<T> create(String queryString) {
      return new FilterQuery<>(this, queryString, null, null, -1, -1);
   }

   @Override
   public QueryBuilder from(Class<?> entityType) {
      if (serializationContext != null) {
         serializationContext.getMarshaller(entityType);
      }
      return new FilterQueryBuilder(this, entityType.getName());
   }

   @Override
   public QueryBuilder from(String entityType) {
      if (serializationContext != null) {
         serializationContext.getMarshaller(entityType);
      }
      return new FilterQueryBuilder(this, entityType);
   }

   private static final class FilterQueryBuilder extends BaseQueryBuilder {

      private static final Log log = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, FilterQueryBuilder.class.getName());

      FilterQueryBuilder(FilterQueryFactory queryFactory, String rootType) {
         super(queryFactory, rootType);
      }

      @Override
      public <T> Query<T> build() {
         QueryStringCreator generator = new QueryStringCreator();
         String queryString = accept(generator);
         if (log.isTraceEnabled()) {
            log.tracef("Query string : %s", queryString);
         }
         return new FilterQuery<>(queryFactory, queryString, generator.getNamedParameters(), getProjectionPaths(), startOffset, maxResults);
      }
   }

   private static final class FilterQuery<T> extends BaseQuery<T> {

      FilterQuery(QueryFactory queryFactory, String queryString, Map<String, Object> namedParameters, String[] projection, long startOffset, int maxResults) {
         super(queryFactory, queryString, namedParameters, projection, startOffset, maxResults, false);
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
