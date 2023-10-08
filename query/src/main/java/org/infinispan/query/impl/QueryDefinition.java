package org.infinispan.query.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hibernate.search.engine.search.query.SearchQuery;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;
import org.infinispan.util.function.SerializableFunction;

/**
 * Wraps the query to be executed in a cache represented either as an Ickle query String or as a {@link SearchQuery}
 * together with parameters and pagination and sort information.
 *
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.QUERY_DEFINITION)
public final class QueryDefinition {

   @ProtoField(1)
   final String queryString;

   @ProtoField(2)
   final IckleParsingResult.StatementType statementType;

   @ProtoField(3)
   int maxResults;

   @ProtoField(4)
   int firstResult = 0;

   @ProtoField(5)
   int hitCountAccuracy = -1;

   @ProtoField(6)
   long timeout = -1;

   @ProtoField(7)
   boolean scoreRequired = false;

   private final SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> queryEngineProvider;
   private final Map<String, Object> namedParameters = new HashMap<>();
   private final int originalMaxResults;
   private SearchQueryBuilder searchQueryBuilder;

   public QueryDefinition(String queryString, IckleParsingResult.StatementType statementType,
                          SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> queryEngineProvider,
                          int originalMaxResults) {
      if (queryString == null) {
         throw new IllegalArgumentException("queryString cannot be null");
      }
      if (statementType == null) {
         throw new IllegalArgumentException("statementType cannot be null");
      }
      if (queryEngineProvider == null) {
         throw new IllegalArgumentException("queryEngineProvider cannot be null");
      }
      this.queryString = queryString;
      this.statementType = statementType;
      this.queryEngineProvider = queryEngineProvider;
      this.maxResults = originalMaxResults;
      this.originalMaxResults = originalMaxResults;
   }

   public QueryDefinition(String queryString, IckleParsingResult.StatementType statementType,
                          SearchQueryBuilder searchQueryBuilder, int originalMaxResults) {
      if (queryString == null) {
         throw new IllegalArgumentException("queryString cannot be null");
      }
      if (statementType == null) {
         throw new IllegalArgumentException("statementType cannot be null");
      }
      if (searchQueryBuilder == null) {
         throw new IllegalArgumentException("searchQueryBuilder cannot be null");
      }
      this.searchQueryBuilder = searchQueryBuilder;
      this.queryString = queryString;
      this.statementType = statementType;
      this.queryEngineProvider = null;
      this.maxResults = originalMaxResults;
      this.originalMaxResults = originalMaxResults;
   }

   @ProtoFactory
   static QueryDefinition protoFactory(String queryString, IckleParsingResult.StatementType statementType, int maxResults,
                                       int firstResult, int hitCountAccuracy, long timeout, boolean scoreRequired,
                                       MarshallableObject<SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>>> wrappedQueryEngineProvider,
                                       MarshallableMap<String, Object> wrappedNamedParameters) {
      QueryDefinition queryDefinition = new QueryDefinition(queryString, statementType, MarshallableObject.unwrap(wrappedQueryEngineProvider), maxResults);
      queryDefinition.setFirstResult(firstResult);
      queryDefinition.setHitCountAccuracy(hitCountAccuracy);
      queryDefinition.timeout = timeout;
      queryDefinition.scoreRequired = scoreRequired;
      queryDefinition.setNamedParameters(MarshallableMap.unwrap(wrappedNamedParameters));
      return queryDefinition;
   }

   @ProtoField(number = 8, name = "queryEngineProvider")
   MarshallableObject<SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>>> getWrappedQueryEngineProvider() {
      return MarshallableObject.create(queryEngineProvider);
   }

   @ProtoField(9)
   MarshallableMap<String, Object> getWrappedNamedParameters() {
      return MarshallableMap.create(namedParameters);
   }

   public String getQueryString() {
      return queryString;
   }

   public IckleParsingResult.StatementType getStatementType() {
      return statementType;
   }

   private QueryEngine<?> getQueryEngine(AdvancedCache<?, ?> cache) {
      if (queryEngineProvider == null) {
         throw new IllegalStateException("No query engine provider specified");
      }
      QueryEngine<?> queryEngine = queryEngineProvider.apply(cache);
      if (queryEngine == null) {
         throw new IllegalStateException("The query engine provider could not locate a suitable query engine");
      }
      return queryEngine;
   }

   public void initialize(AdvancedCache<?, ?> cache) {
      if (searchQueryBuilder == null) {
         QueryEngine<?> queryEngine = getQueryEngine(cache);
         searchQueryBuilder = queryEngine.buildSearchQuery(queryString, namedParameters);
         if (hitCountAccuracy != -1) {
            searchQueryBuilder.hitCountAccuracy(hitCountAccuracy);
         }
         if (timeout > 0) {
            searchQueryBuilder.failAfter(timeout, TimeUnit.NANOSECONDS);
         }
      }
   }

   public SearchQueryBuilder getSearchQueryBuilder() {
      if (searchQueryBuilder == null) {
         throw new IllegalStateException("The QueryDefinition has not been initialized, make sure to call initialize(...) first");
      }
      return searchQueryBuilder;
   }

   public boolean isCustomMaxResults() {
      return maxResults != originalMaxResults;
   }

   public int getMaxResults() {
      return maxResults == -1 ? Integer.MAX_VALUE - getFirstResult() : maxResults;
   }

   public void setMaxResults(int maxResults) {
      this.maxResults = maxResults;
   }

   public void setHitCountAccuracy(int hitCountAccuracy) {
      this.hitCountAccuracy = hitCountAccuracy;
      if (this.hitCountAccuracy != -1 && searchQueryBuilder != null) {
         searchQueryBuilder.hitCountAccuracy(hitCountAccuracy);
      }
   }

   public void setNamedParameters(Map<String, Object> params) {
      if (params == null) {
         namedParameters.clear();
      } else {
         namedParameters.putAll(params);
      }
   }

   public void setTimeout(long timeout, TimeUnit timeUnit) {
      this.timeout = timeUnit.toNanos(timeout);
      if (this.timeout > 0 && searchQueryBuilder != null) {
         searchQueryBuilder.failAfter(this.timeout, TimeUnit.NANOSECONDS);
      }
   }

   public Map<String, Object> getNamedParameters() {
      return namedParameters;
   }

   public int getFirstResult() {
      return firstResult;
   }

   public void setFirstResult(int firstResult) {
      this.firstResult = firstResult;
   }

   public void failAfter(long timeout, TimeUnit timeUnit) {
      getSearchQueryBuilder().failAfter(timeout, timeUnit);
   }

   public void scoreRequired() {
      this.scoreRequired = true;
   }

   public boolean isScoreRequired() {
      return this.scoreRequired;
   }
}
