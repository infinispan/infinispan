package org.infinispan.query.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.hibernate.search.engine.search.query.SearchQuery;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;
import org.infinispan.query.impl.externalizers.ExternalizerIds;
import org.infinispan.util.function.SerializableFunction;

/**
 * Wraps the query to be executed in a cache represented either as an Ickle query String or as a {@link SearchQuery}
 * together with parameters and pagination and sort information.
 *
 * @since 9.2
 */
public final class QueryDefinition {

   private final SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> queryEngineProvider;
   private final String queryString;
   private final IckleParsingResult.StatementType statementType;
   private SearchQueryBuilder searchQueryBuilder;
   private int maxResults;
   private int firstResult = 0;
   private long timeout = -1;

   private final Map<String, Object> namedParameters = new HashMap<>();
   private final int originalMaxResults;

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

   public static final class Externalizer implements AdvancedExternalizer<QueryDefinition> {

      @Override
      public Set<Class<? extends QueryDefinition>> getTypeClasses() {
         return Collections.singleton(QueryDefinition.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.QUERY_DEFINITION;
      }

      @Override
      public void writeObject(ObjectOutput output, QueryDefinition queryDefinition) throws IOException {
         output.writeUTF(queryDefinition.queryString);
         output.writeByte(queryDefinition.statementType.ordinal());
         output.writeObject(queryDefinition.queryEngineProvider);
         output.writeInt(queryDefinition.firstResult);
         output.writeInt(queryDefinition.maxResults);
         output.writeLong(queryDefinition.timeout);
         Map<String, Object> namedParameters = queryDefinition.namedParameters;
         int paramSize = namedParameters.size();
         output.writeShort(paramSize);
         if (paramSize != 0) {
            for (Map.Entry<String, Object> param : namedParameters.entrySet()) {
               output.writeUTF(param.getKey());
               output.writeObject(param.getValue());
            }
         }
      }

      @Override
      public QueryDefinition readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String queryString = input.readUTF();
         IckleParsingResult.StatementType statementType = IckleParsingResult.StatementType.valueOf(input.readByte());
         SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> engineProvider =
               (SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>>) input.readObject();

         int firstResult = input.readInt();
         int maxResults = input.readInt();

         // maxResults becomes the originalMaxResults of the distributed cloned queries
         QueryDefinition queryDefinition = new QueryDefinition(queryString, statementType, engineProvider, maxResults);
         queryDefinition.setFirstResult(firstResult);

         queryDefinition.timeout = input.readLong();
         short paramSize = input.readShort();
         if (paramSize != 0) {
            Map<String, Object> params = new HashMap<>(paramSize);
            for (int i = 0; i < paramSize; i++) {
               String key = input.readUTF();
               Object value = input.readObject();
               params.put(key, value);
            }
            queryDefinition.setNamedParameters(params);
         }
         return queryDefinition;
      }
   }
}
