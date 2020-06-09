package org.infinispan.query.dsl.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public abstract class BaseQuery<T> implements Query<T> {

   private static final Log log = Logger.getMessageLogger(Log.class, BaseQuery.class.getName());

   protected final QueryFactory queryFactory;

   protected final String queryString;

   private final boolean paramsDefined;

   protected Map<String, Object> namedParameters;

   protected final String[] projection;

   protected int startOffset;

   protected int maxResults;

   /**
    * Optional timeout in nanoseconds.
    */
   protected long timeout = -1;

   //todo [anistor] can startOffset really be a long or it really has to be int due to limitations in query module?
   protected BaseQuery(QueryFactory queryFactory, String queryString,
                       Map<String, Object> namedParameters, String[] projection, long startOffset, int maxResults) {
      this.paramsDefined = true;
      this.queryFactory = queryFactory;
      this.queryString = queryString;
      this.namedParameters = namedParameters;
      this.projection = projection != null && projection.length > 0 ? projection : null;
      this.startOffset = startOffset < 0 ? 0 : (int) startOffset;
      this.maxResults = maxResults;
   }

   protected BaseQuery(QueryFactory queryFactory, String queryString) {
      this.paramsDefined = false;
      this.queryFactory = queryFactory;
      this.queryString = queryString;
      this.projection = null;
      this.startOffset = 0;
      this.maxResults = -1;
   }

   /**
    * Returns the Ickle query string.
    *
    * @return the Ickle query string
    */
   @Override
   public String getQueryString() {
      return queryString;
   }

   @Override
   public Map<String, Object> getParameters() {
      return namedParameters != null ? Collections.unmodifiableMap(namedParameters) : null;
   }

   @Override
   public Query<T> setParameter(String paramName, Object paramValue) {
      if (paramName == null || paramName.isEmpty()) {
         throw log.parameterNameCannotBeNulOrEmpty();
      }
      if (paramsDefined) {
         if (namedParameters == null) {
            throw log.queryDoesNotHaveParameters();
         }
         if (!namedParameters.containsKey(paramName)) {
            throw log.parameterNotFound(paramName);
         }
      } else if (namedParameters == null) {
         namedParameters = new HashMap<>(5);
      }

      namedParameters.put(paramName, paramValue);

      // reset the query to force a new execution
      resetQuery();

      return this;
   }

   @Override
   public Query<T> setParameters(Map<String, Object> paramValues) {
      if (paramValues == null) {
         throw log.argumentCannotBeNull("paramValues");
      }
      if (paramsDefined) {
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
      } else if (namedParameters == null) {
         namedParameters = new HashMap<>(5);
      }

      namedParameters.putAll(paramValues);

      // reset the query to force a new execution
      resetQuery();

      return this;
   }

   /**
    * Reset internal state after pagination or query parameters are modified. This is needed to ensure the next
    * execution of the query uses the new values.
    */
   public abstract void resetQuery();

   /**
    * Ensure all named parameters have non-null values.
    */
   public void validateNamedParameters() {
      if (namedParameters != null) {
         for (Map.Entry<String, Object> e : namedParameters.entrySet()) {
            if (e.getValue() == null) {
               throw log.queryParameterNotSet(e.getKey());
            }
         }
      }
   }

   public String[] getProjection() {
      return projection;
   }

   @Override
   public boolean hasProjections() {
      return projection != null;
   }

   @Override
   public long getStartOffset() {
      return startOffset;
   }

   public int getMaxResults() {
      return maxResults;
   }

   @Override
   public Query<T> startOffset(long startOffset) {
      this.startOffset = (int) startOffset;    //todo [anistor] why int?
      resetQuery();
      return this;
   }

   @Override
   public Query<T> maxResults(int maxResults) {
      this.maxResults = maxResults;
      resetQuery();
      return this;
   }

   @Override
   public Query<T> timeout(long timeout, TimeUnit timeUnit) {
      this.timeout = timeUnit.toNanos(timeout);
      return this;
   }
}
