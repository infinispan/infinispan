package org.infinispan.query.dsl.embedded.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
final class DelegatingQuery<TypeMetadata> extends BaseQuery {

   private static final Log log = Logger.getMessageLogger(Log.class, DelegatingQuery.class.getName());

   private final QueryEngine<TypeMetadata> queryEngine;
   private final IndexedQueryMode queryMode;

   private final IckleParsingResult<TypeMetadata> parsingResult;

   /**
    * The actual query object to which execution will be delegated.
    */
   private BaseQuery query;

   DelegatingQuery(QueryEngine<TypeMetadata> queryEngine, QueryFactory queryFactory, String queryString, IndexedQueryMode queryMode) {
      super(queryFactory, queryString);
      this.queryEngine = queryEngine;
      this.queryMode = queryMode;

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
                   Map<String, Object> namedParameters, String[] projection, long startOffset, int maxResults) {
      super(queryFactory, queryString, namedParameters, projection, startOffset, maxResults);
      this.queryEngine = queryEngine;
      this.queryMode = IndexedQueryMode.FETCH;

      // parse and validate early
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
      if (query != null) {
         // reset the delegate but do not discard it!
         query.resetQuery();
      }
   }

   private Query createQuery() {
      // the query is created first time only
      if (query == null) {
         query = queryEngine.buildQuery(queryFactory, parsingResult, namedParameters, startOffset, maxResults, queryMode);
      }
      return query;
   }

   @Override
   public <T> List<T> list() {
      return createQuery().list();
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
            '}';
   }
}
