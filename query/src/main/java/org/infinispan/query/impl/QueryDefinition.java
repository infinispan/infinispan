package org.infinispan.query.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.hibernate.search.filter.FullTextFilter;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.hibernate.search.spi.CustomTypeMetadata;
import org.hibernate.search.spi.IndexedTypeMap;
import org.hibernate.search.spi.impl.IndexedTypeMaps;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.query.dsl.embedded.impl.HsQueryRequest;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.impl.externalizers.ExternalizerIds;
import org.infinispan.query.logging.Log;
import org.infinispan.util.function.SerializableFunction;
import org.infinispan.util.logging.LogFactory;

/**
 * Wraps the query to be executed in a cache represented either as a String or as a {@link HSQuery} form together with
 * pagination and sort information.
 *
 * @since 9.2
 */
public final class QueryDefinition {

   private static final Log log = LogFactory.getLog(QueryDefinition.class, Log.class);

   private final SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> queryEngineProvider;
   private final String queryString;
   private HSQuery hsQuery;
   private Query luceneQuery;
   private int maxResults = 100;
   private int firstResult;
   private Set<String> sortableFields;
   private Class<?> indexedType;
   private Sort sort;

   private final Map<String, Object> namedParameters = new HashMap<>();

   public QueryDefinition(String queryString, SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> queryEngineProvider) {
      if (queryString == null) {
         throw new IllegalArgumentException("queryString cannot be null");
      }
      if (queryEngineProvider == null) {
         throw new IllegalArgumentException("queryEngineProvider cannot be null");
      }
      this.queryString = queryString;
      this.queryEngineProvider = queryEngineProvider;
      this.luceneQuery = null;
   }

   private QueryDefinition(Query query, Sort sort) {
      if (query == null) {
         throw new IllegalArgumentException("query cannot be null");
      }
      this.luceneQuery = query;
      this.sort = sort;
      this.queryString = null;
      this.queryEngineProvider = null;
   }

   public QueryDefinition(HSQuery hsQuery) {
      if (hsQuery == null) {
         throw new IllegalArgumentException("hsQuery cannot be null");
      }
      this.hsQuery = hsQuery;
      this.queryString = null;
      this.queryEngineProvider = null;
   }

   public String getQueryString() {
      return queryString;
   }

   private IndexedTypeMap<CustomTypeMetadata> createMetadata() {
      return IndexedTypeMaps.singletonMapping(PojoIndexedTypeIdentifier.convertFromLegacy(indexedType), () -> sortableFields);
   }

   private QueryEngine getQueryEngine(AdvancedCache<?, ?> cache) {
      if (queryEngineProvider == null) {
         throw new IllegalStateException("No query engine provider specified");
      }
      QueryEngine queryEngine = queryEngineProvider.apply(cache);
      if (queryEngine == null) {
         throw new IllegalStateException("The provider could not locate a suitable query engine");
      }
      return queryEngine;
   }

   public void initialize(AdvancedCache<?, ?> cache) {
      if (hsQuery == null) {
         if (luceneQuery != null) {
            hsQuery = ComponentRegistryUtils.getSearchIntegrator(cache).createHSQuery(luceneQuery);
            if (sort != null)
               hsQuery.sort(sort);
         } else {
            QueryEngine queryEngine = getQueryEngine(cache);
            HsQueryRequest hsQueryRequest;
            if (indexedType != null && sortableFields != null) {
               IndexedTypeMap<CustomTypeMetadata> metadata = createMetadata();
               hsQueryRequest = queryEngine.createHsQuery(queryString, metadata, namedParameters);
            } else {
               hsQueryRequest = queryEngine.createHsQuery(queryString, null, namedParameters);
            }
            hsQuery = hsQueryRequest.getHsQuery();
            sort = hsQueryRequest.getSort();
            hsQuery.projection(hsQueryRequest.getProjections());
         }
         hsQuery.firstResult(firstResult);
         hsQuery.maxResults(maxResults);
      }
   }

   public HSQuery getHsQuery() {
      if (hsQuery == null) {
         throw new IllegalStateException("The QueryDefinition has not been initialized, make sure to call initialize(...) first");
      }
      return hsQuery;
   }

   public int getMaxResults() {
      return maxResults;
   }

   public void setMaxResults(int maxResults) {
      this.maxResults = maxResults;
      if (hsQuery != null) {
         hsQuery.maxResults(maxResults);
      }
   }

   public void setNamedParameters(Map<String, Object> params) {
      if (params == null) {
         namedParameters.clear();
      } else {
         namedParameters.putAll(params);
      }
   }

   public Map<String, Object> getNamedParameters() {
      return namedParameters;
   }

   public int getFirstResult() {
      return firstResult;
   }

   public void setFirstResult(int firstResult) {
      if (hsQuery != null) {
         hsQuery.firstResult(firstResult);
      }
      this.firstResult = firstResult;
   }

   public Sort getSort() {
      return sort;
   }

   public void setSort(Sort sort) {
      if (queryString != null) {
         throw log.sortNotSupportedWithQueryString();
      }
      hsQuery.sort(sort);
      this.sort = sort;
   }

   public void filter(Filter filter) {
      if (queryString != null) throw log.filterNotSupportedWithQueryString();
      hsQuery.filter(filter);
   }

   public FullTextFilter enableFullTextFilter(String name) {
      if (queryString != null) throw log.filterNotSupportedWithQueryString();
      return hsQuery.enableFullTextFilter(name);
   }

   public void disableFullTextFilter(String name) {
      if (queryString != null) throw log.filterNotSupportedWithQueryString();
      hsQuery.disableFullTextFilter(name);
   }

   public Set<String> getSortableFields() {
      return sortableFields;
   }

   public void setSortableField(Set<String> sortableField) {
      this.sortableFields = sortableField;
   }

   public Class<?> getIndexedType() {
      return indexedType;
   }

   public void setIndexedType(Class<?> indexedType) {
      this.indexedType = indexedType;
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
         if (queryDefinition.queryString != null) {
            output.writeBoolean(true);
            output.writeUTF(queryDefinition.queryString);
            output.writeObject(queryDefinition.queryEngineProvider);
         } else {
            output.writeBoolean(false);
            output.writeObject(queryDefinition.hsQuery.getLuceneQuery());
            output.writeObject(queryDefinition.sort);
         }
         output.writeInt(queryDefinition.firstResult);
         output.writeInt(queryDefinition.maxResults);
         output.writeObject(queryDefinition.sortableFields);
         output.writeObject(queryDefinition.indexedType);
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
         QueryDefinition queryDefinition;
         if (input.readBoolean()) {
            String queryString = input.readUTF();
            SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> queryEngineProvider = (SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>>) input.readObject();
            queryDefinition = new QueryDefinition(queryString, queryEngineProvider);
         } else {
            Query query = (Query) input.readObject();
            Sort sort = (Sort) input.readObject();
            queryDefinition = new QueryDefinition(query, sort);
         }
         queryDefinition.setFirstResult(input.readInt());
         queryDefinition.setMaxResults(input.readInt());
         Set<String> sortableField = (Set<String>) input.readObject();
         Class<?> indexedTypes = (Class<?>) input.readObject();
         queryDefinition.setSortableField(sortableField);
         queryDefinition.setIndexedType(indexedTypes);
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
