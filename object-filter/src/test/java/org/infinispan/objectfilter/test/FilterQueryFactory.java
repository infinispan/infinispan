package org.infinispan.objectfilter.test;

import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;
import org.infinispan.query.dsl.impl.BaseQueryFactory;
import org.infinispan.query.dsl.impl.JPAQueryGenerator;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public final class FilterQueryFactory extends BaseQueryFactory {

   private final SerializationContext serializationContext;

   public FilterQueryFactory(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   public FilterQueryFactory() {
      this(null);
   }

   @Override
   public QueryBuilder from(Class entityType) {
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
         JPAQueryGenerator generator = new JPAQueryGenerator();
         String jpqlString = accept(generator);
         if (log.isTraceEnabled()) {
            log.tracef("JPQL string : %s", jpqlString);
         }
         return new FilterQuery(queryFactory, jpqlString, generator.getNamedParameters(), getProjectionPaths(), startOffset, maxResults);
      }
   }

   private static final class FilterQuery extends BaseQuery {

      FilterQuery(QueryFactory queryFactory, String jpaQuery, Map<String, Object> namedParameters, String[] projection, long startOffset, int maxResults) {
         super(queryFactory, jpaQuery, namedParameters, projection, startOffset, maxResults);
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
