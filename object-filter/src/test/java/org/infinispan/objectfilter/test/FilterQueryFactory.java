package org.infinispan.objectfilter.test;

import java.util.List;
import java.util.Map;

import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
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
   public Query create(String queryString) {
      return new FilterQuery(this, queryString, null, null, -1, -1);
   }

   @Override
   public Query create(String queryString, IndexedQueryMode queryMode) {
      return new FilterQuery(this, queryString, null, null, -1, -1);
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

      private static final Log log = Logger.getMessageLogger(Log.class, FilterQueryBuilder.class.getName());

      FilterQueryBuilder(FilterQueryFactory queryFactory, String rootType) {
         super(queryFactory, rootType);
      }

      @Override
      public Query build() {
         QueryStringCreator generator = new QueryStringCreator();
         String queryString = accept(generator);
         if (log.isTraceEnabled()) {
            log.tracef("Query string : %s", queryString);
         }
         return new FilterQuery(queryFactory, queryString, generator.getNamedParameters(), getProjectionPaths(), startOffset, maxResults);
      }
   }

   private static final class FilterQuery extends BaseQuery {

      FilterQuery(QueryFactory queryFactory, String queryString, Map<String, Object> namedParameters, String[] projection, long startOffset, int maxResults) {
         super(queryFactory, queryString, namedParameters, projection, startOffset, maxResults);
      }

      @Override
      public void resetQuery() {
      }

      // TODO [anistor] need to rethink the dsl Query/QueryBuilder interfaces to accommodate the filtering scenario ...
      @Override
      public <T> List<T> list() {
         throw new UnsupportedOperationException();
      }

      @Override
      public int getResultSize() {
         throw new UnsupportedOperationException();
      }
   }
}
