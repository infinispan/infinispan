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

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public final class FilterQueryFactory extends BaseQueryFactory<Query> {

   private final SerializationContext serializationContext;

   public FilterQueryFactory(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   public FilterQueryFactory() {
      this(null);
   }

   @Override
   public QueryBuilder<Query> from(Class entityType) {
      if (serializationContext != null) {
         serializationContext.getMarshaller(entityType);
      }
      return new FilterQueryBuilder(this, entityType.getCanonicalName());
   }

   @Override
   public QueryBuilder<Query> from(String entityType) {
      if (serializationContext != null) {
         serializationContext.getMarshaller(entityType);
      }
      return new FilterQueryBuilder(this, entityType);
   }

   private static final class FilterQueryBuilder extends BaseQueryBuilder<Query> {

      private static final Log log = Logger.getMessageLogger(Log.class, FilterQueryBuilder.class.getName());

      FilterQueryBuilder(FilterQueryFactory queryFactory, String rootType) {
         super(queryFactory, rootType);
      }

      @Override
      public Query build() {
         String jpqlString = accept(new JPAQueryGenerator());
         if (log.isTraceEnabled()) {
            log.tracef("JPQL string : %s", jpqlString);
         }
         return new FilterQuery(queryFactory, jpqlString, projection);
      }
   }

   private static final class FilterQuery extends BaseQuery {

      FilterQuery(QueryFactory queryFactory, String jpaQuery, String[] projection) {
         super(queryFactory, jpaQuery, projection);
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
