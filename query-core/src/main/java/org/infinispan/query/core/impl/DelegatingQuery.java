package org.infinispan.query.core.impl;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.api.query.ClosableIteratorWithCount;
import org.infinispan.commons.api.query.EntityEntry;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
final class DelegatingQuery<TypeMetadata, T> extends BaseQuery<T> {

   private static final Log log = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, DelegatingQuery.class.getName());

   private final QueryEngine<TypeMetadata> queryEngine;

   private final IckleParsingResult<TypeMetadata> parsingResult;

   /**
    * The actual query object to which execution will be delegated. This is created in {@link #createQuery()} method.
    */
   private Query<T> query;

   DelegatingQuery(QueryEngine<TypeMetadata> queryEngine, QueryFactory queryFactory, String queryString) {
      super(queryFactory, queryString);
      this.queryEngine = queryEngine;

      // parse and validate early
      parsingResult = queryEngine.parse(queryString);

      if (!parsingResult.getParameterNames().isEmpty()) {
         namedParameters = new HashMap<>(parsingResult.getParameterNames().size());
         for (String paramName : parsingResult.getParameterNames()) {
            namedParameters.put(paramName, null);
         }
      }
   }

   DelegatingQuery(QueryEngine<TypeMetadata> queryEngine, QueryFactory queryFactory, String queryString,
                   Map<String, Object> namedParameters, String[] projection, long startOffset, int maxResults, boolean local) {
      super(queryFactory, queryString, namedParameters, projection, startOffset, maxResults, local);
      this.queryEngine = queryEngine;

      // parse and validate early; we also discover the param names
      parsingResult = queryEngine.parse(queryString);

      if (namedParameters != null) {
         List<String> unknownParams = null;
         for (String paramName : namedParameters.keySet()) {
            if (paramName == null || paramName.isEmpty()) {
               throw log.parameterNameCannotBeNulOrEmpty();
            }
            if (!parsingResult.getParameterNames().contains(paramName)) {
               if (unknownParams == null) {
                  unknownParams = new ArrayList<>();
               }
               unknownParams.add(paramName);
            }
         }
         if (unknownParams != null) {
            throw log.parametersNotFound(unknownParams.toString());
         }
      }
   }

   @Override
   public String[] getProjection() {
      return parsingResult.getProjections();
   }

   @Override
   public void resetQuery() {
      query = null;
   }

   private Query<T> createQuery() {
      // the query is created first time only
      if (query == null) {
         query = (Query<T>) queryEngine.buildQuery(queryFactory, parsingResult, namedParameters, startOffset, maxResults, local);
         if (timeout > 0) {
            query.timeout(timeout, TimeUnit.NANOSECONDS);
         }
         if (hitCountAccuracy != null) {
            query.hitCountAccuracy(hitCountAccuracy);
         }
      }
      return query;
   }

   @Override
   public List<T> list() {
      return createQuery().list();
   }

   @Override
   public QueryResult<T> execute() {
      return createQuery().execute();
   }

   @Override
   public CompletionStage<org.infinispan.commons.api.query.QueryResult<T>> executeAsync() {
      return createQuery().executeAsync();
   }

   @Override
   public int executeStatement() {
      return createQuery().executeStatement();
   }

   @Override
   public CompletionStage<Integer> executeStatementAsync() {
      return createQuery().executeStatementAsync();
   }

   @Override
   public CloseableIterator<T> iterator() {
      return createQuery().iterator();
   }

   @Override
   public <K> ClosableIteratorWithCount<EntityEntry<K, T>> entryIterator(boolean withMetadata) {
      return createQuery().entryIterator(withMetadata);
   }

   @Override
   public int getResultSize() {
      return createQuery().getResultSize();
   }

   @Override
   public String toString() {
      return "DelegatingQuery{" +
            "queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", timeout=" + timeout +
            '}';
   }
}
