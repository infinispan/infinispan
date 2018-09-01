package org.infinispan.query.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Sort;
import org.hibernate.search.filter.FullTextFilter;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.hibernate.search.spi.CustomTypeMetadata;
import org.hibernate.search.spi.IndexedTypeMap;
import org.hibernate.search.spi.impl.IndexedTypeMaps;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQueryEngine;
import org.infinispan.query.dsl.embedded.impl.HsQueryRequest;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.impl.externalizers.ExternalizerIds;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Wraps the query to be executed in a cache represented either as a String or as a {@link HSQuery} form together with
 * pagination and sort information.
 *
 * @since 9.2
 */
public class QueryDefinition {

   private static final Log log = LogFactory.getLog(QueryDefinition.class, Log.class);

   private String queryString;
   private HSQuery hsQuery;
   private int maxResults = 100;
   private int firstResult;
   private Set<String> sortableFields;
   private Class<?> indexedType;

   private final Map<String, Object> namedParameters = new HashMap<>();
   private transient Sort sort;

   public QueryDefinition(String queryString) {
      this.queryString = queryString;
   }

   public QueryDefinition(HSQuery hsQuery) {
      this.hsQuery = hsQuery;
   }

   public Optional<String> getQueryString() {
      return Optional.ofNullable(queryString);
   }

   private IndexedTypeMap<CustomTypeMetadata> createMetadata() {
      return IndexedTypeMaps.singletonMapping(PojoIndexedTypeIdentifier.convertFromLegacy(indexedType), () -> sortableFields);
   }

   protected QueryEngine getQueryEngine(AdvancedCache<?, ?> cache) {
      return cache.getComponentRegistry().getComponent(EmbeddedQueryEngine.class);
   }

   public void initialize(AdvancedCache<?, ?> cache) {
      if (hsQuery == null) {
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
         hsQuery.firstResult(firstResult);
         hsQuery.maxResults(maxResults);
         hsQuery.projection(hsQueryRequest.getProjections());
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

   public static class Externalizer implements AdvancedExternalizer<QueryDefinition> {

      @Override
      public Set<Class<? extends QueryDefinition>> getTypeClasses() {
         return Collections.singleton(QueryDefinition.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.QUERY_DEFINITION;
      }

      @Override
      public void writeObject(ObjectOutput output, QueryDefinition object) throws IOException {
         if (object.getQueryString().isPresent()) {
            output.writeBoolean(true);
            output.writeUTF(object.getQueryString().get());
         } else {
            output.writeBoolean(false);
            output.writeObject(object.getHsQuery());
         }
         output.writeInt(object.getFirstResult());
         output.writeInt(object.getMaxResults());
         output.writeObject(object.getSortableFields());
         output.writeObject(object.getIndexedType());
         Map<String, Object> namedParameters = object.getNamedParameters();
         int size = namedParameters.size();
         output.writeShort(size);
         for (Map.Entry<String, Object> param : namedParameters.entrySet()) {
            output.writeUTF(param.getKey());
            output.writeObject(param.getValue());
         }
      }

      @Override
      public QueryDefinition readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         QueryDefinition queryDefinition;
         if (input.readBoolean()) {
            queryDefinition = createQueryDefinition(input.readUTF());
         } else {
            queryDefinition = createQueryDefinition((HSQuery) input.readObject());
         }
         queryDefinition.setFirstResult(input.readInt());
         queryDefinition.setMaxResults(input.readInt());
         Set<String> sortableField = (Set<String>) input.readObject();
         Class<?> indexedTypes = (Class<?>) input.readObject();
         queryDefinition.setSortableField(sortableField);
         queryDefinition.setIndexedType(indexedTypes);
         short paramSize = input.readShort();
         if (paramSize > 0) {
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

      protected QueryDefinition createQueryDefinition(String queryString) {
         return new QueryDefinition(queryString);
      }

      protected QueryDefinition createQueryDefinition(HSQuery hsQuery) {
         return new QueryDefinition(hsQuery);
      }
   }
}
