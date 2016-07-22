package org.infinispan.query.dsl.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public abstract class BaseQuery implements Query {

   private static final Log log = Logger.getMessageLogger(Log.class, BaseQuery.class.getName());

   protected final QueryFactory queryFactory;

   protected final String jpaQuery;

   protected final Map<String, Object> namedParameters;

   protected final String[] projection;

   protected final int startOffset;

   protected final int maxResults;

   //todo [anistor] can startOffset really be a long or it really has to be int due to limitations in query module?
   protected BaseQuery(QueryFactory queryFactory, String jpaQuery, Map<String, Object> namedParameters, String[] projection,
                       long startOffset, int maxResults) {
      this.queryFactory = queryFactory;
      this.jpaQuery = jpaQuery;
      this.namedParameters = namedParameters;
      this.projection = projection != null && projection.length > 0 ? projection : null;
      this.startOffset = startOffset < 0 ? 0 : (int) startOffset;
      this.maxResults = maxResults;
   }

   public QueryFactory getQueryFactory() {
      return queryFactory;
   }

   public String getJPAQuery() {
      return jpaQuery;
   }

   @Override
   public Map<String, Object> getParameters() {
      return Collections.unmodifiableMap(namedParameters);
   }

   @Override
   public Query setParameter(String paramName, Object paramValue) {
      if (namedParameters == null) {
         throw log.queryDoesNotHaveParameters();
      }
      if (paramName == null || paramName.isEmpty()) {
         throw log.parameterNameCannotBeNulOrEmpty();
      }
      if (!namedParameters.containsKey(paramName)) {
         throw log.parameterNotFound(paramName);
      }
      namedParameters.put(paramName, paramValue);

      // reset the query to force a new execution
      resetQuery();

      return this;
   }

   @Override
   public Query setParameters(Map<String, Object> paramValues) {
      if (paramValues == null) {
         throw log.argumentCannotBeNull("paramValues");
      }
      if (namedParameters == null) {
         throw log.queryDoesNotHaveParameters();
      }
      List<String> unknownParams = null;
      for (String paramName : paramValues.keySet()) {
         if (paramName == null || paramName.isEmpty()) {
            throw log.parameterNameCannotBeNulOrEmpty();
         }
         if (!namedParameters.containsKey(paramName)) {
            if (unknownParams == null) {
               unknownParams = new ArrayList<>();
            }
            unknownParams.add(paramName);
         }
      }
      if (unknownParams != null) {
         throw log.parametersNotFound(unknownParams.toString());
      }
      namedParameters.putAll(paramValues);

      // reset the query to force a new execution
      resetQuery();

      return this;
   }

   /**
    * Reset internal state after query parameters are modified. This is needed to ensure the next execution of the query
    * uses the new parameter values.
    */
   public abstract void resetQuery();

   public Map<String, Object> getNamedParameters() {
      return namedParameters;
   }

   public String[] getProjection() {
      return projection;
   }

   public long getStartOffset() {
      return startOffset;
   }

   public int getMaxResults() {
      return maxResults;
   }
}
