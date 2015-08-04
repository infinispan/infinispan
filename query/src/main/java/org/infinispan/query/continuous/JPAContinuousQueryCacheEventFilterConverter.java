package org.infinispan.query.continuous;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.dsl.embedded.impl.QueryCache;
import org.infinispan.query.impl.externalizers.ExternalizerIds;
import org.infinispan.util.KeyValuePair;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class JPAContinuousQueryCacheEventFilterConverter<K, V> extends AbstractCacheEventFilterConverter<K, V, ContinuousQueryResult<V>> {

   /**
    * Optional cache for query objects.
    */
   private QueryCache queryCache;

   /**
    * The JPA query to execute.
    */
   private final String jpaQuery;

   private final Map<String, Object> namedParameters;

   /**
    * The implementation class of the Matcher component to lookup and use.
    */
   private final Class<? extends Matcher> matcherImplClass;

   /**
    * The Matcher, acquired via dependency injection.
    */
   private Matcher matcher;

   /**
    * The ObjectFilter is created lazily.
    */
   private ObjectFilter objectFilter;

   public JPAContinuousQueryCacheEventFilterConverter(String jpaQuery, Map<String, Object> namedParameters, Class<? extends Matcher> matcherImplClass) {
      if (jpaQuery == null || matcherImplClass == null) {
         throw new IllegalArgumentException("Arguments cannot be null");
      }
      this.jpaQuery = jpaQuery;
      this.namedParameters = namedParameters;
      this.matcherImplClass = matcherImplClass;
   }

   /**
    * Acquires a Matcher instance from the ComponentRegistry of the given Cache object.
    */
   @Inject
   public void injectDependencies(Cache cache) {
      this.queryCache = cache.getCacheManager().getGlobalComponentRegistry().getComponent(QueryCache.class);
      matcher = cache.getAdvancedCache().getComponentRegistry().getComponent(matcherImplClass);
      if (matcher == null) {
         throw new CacheException("Expected component not found in registry: " + matcherImplClass.getName());
      }
   }

   private ObjectFilter getObjectFilter() {
      if (objectFilter == null) {
         // if parameters are present caching cannot be currently performed due to internal implementation limitations
         if (queryCache != null && (namedParameters == null || namedParameters.isEmpty())) {
            KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpaQuery, matcherImplClass);
            ObjectFilter objectFilter = queryCache.get(queryCacheKey);
            if (objectFilter == null) {
               objectFilter = matcher.getObjectFilter(jpaQuery, namedParameters);
               queryCache.put(queryCacheKey, objectFilter);
            }
            this.objectFilter = objectFilter;
         } else {
            objectFilter = matcher.getObjectFilter(jpaQuery, namedParameters);
         }
      }
      return objectFilter;
   }

   private ObjectFilter.FilterResult filterAndConvert(V value) {
      if (value == null) {
         return null;
      }
      return getObjectFilter().filter(value);
   }

   @Override
   public String toString() {
      return "JPAQCCacheEventFilterConverter{jpaQuery='" + jpaQuery + "'}";
   }

   @Override
   public ContinuousQueryResult<V> filterAndConvert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      ObjectFilter.FilterResult f1 = filterAndConvert(oldValue);
      ObjectFilter.FilterResult f2 = filterAndConvert(newValue);

      if (f1 == null && f2 != null) {
         return new ContinuousQueryResult<V>(true, newValue);
      }

      if (f1 != null && f2 == null) {
         return new ContinuousQueryResult<V>(false, null);
      }

      return null;
   }

   public static final class Externalizer extends AbstractExternalizer<JPAContinuousQueryCacheEventFilterConverter> {

      @Override
      public void writeObject(ObjectOutput output, JPAContinuousQueryCacheEventFilterConverter filterAndConverter) throws IOException {
         output.writeUTF(filterAndConverter.jpaQuery);
         Map<String, Object> namedParameters = filterAndConverter.namedParameters;
         if (namedParameters != null) {
            UnsignedNumeric.writeUnsignedInt(output, namedParameters.size());
            for (Map.Entry<String, Object> e : namedParameters.entrySet()) {
               output.writeUTF(e.getKey());
               output.writeObject(e.getValue());
            }
         } else {
            UnsignedNumeric.writeUnsignedInt(output, 0);
         }
         output.writeObject(filterAndConverter.matcherImplClass);
      }

      @Override
      public JPAContinuousQueryCacheEventFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String jpaQuery = input.readUTF();
         int paramsSize = UnsignedNumeric.readUnsignedInt(input);
         Map<String, Object> namedParameters = null;
         if (paramsSize != 0) {
            namedParameters = new HashMap<String, Object>(paramsSize);
            for (int i = 0; i < paramsSize; i++) {
               String paramName = input.readUTF();
               Object paramValue = input.readObject();
               namedParameters.put(paramName, paramValue);
            }
         }
         Class<? extends Matcher> matcherImplClass = (Class<? extends Matcher>) input.readObject();
         return new JPAContinuousQueryCacheEventFilterConverter(jpaQuery, namedParameters, matcherImplClass);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JPA_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER;
      }

      @Override
      public Set<Class<? extends JPAContinuousQueryCacheEventFilterConverter>> getTypeClasses() {
         return Collections.<Class<? extends JPAContinuousQueryCacheEventFilterConverter>>singleton(JPAContinuousQueryCacheEventFilterConverter.class);
      }
   }
}
