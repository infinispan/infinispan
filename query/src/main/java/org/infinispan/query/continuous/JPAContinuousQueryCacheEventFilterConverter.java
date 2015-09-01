package org.infinispan.query.continuous;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.event.Event;
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

   public JPAContinuousQueryCacheEventFilterConverter(String jpaQuery, Class<? extends Matcher> matcherImplClass) {
      if (jpaQuery == null || matcherImplClass == null) {
         throw new IllegalArgumentException("Arguments cannot be null");
      }
      this.jpaQuery = jpaQuery;
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
         if (queryCache != null) {
            KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpaQuery, matcherImplClass);
            ObjectFilter objectFilter = queryCache.get(queryCacheKey);
            if (objectFilter == null) {
               objectFilter = matcher.getObjectFilter(jpaQuery);
               queryCache.put(queryCacheKey, objectFilter);
            }
            this.objectFilter = objectFilter;
         } else {
            objectFilter = matcher.getObjectFilter(jpaQuery);
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

      if (f2 != null && eventType.isExpired()) {  // expired events return expired value as newValue
         return new ContinuousQueryResult<V>(false, null);
      }
      
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
         output.writeObject(filterAndConverter.matcherImplClass);
      }

      @Override
      public JPAContinuousQueryCacheEventFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String jpaQuery = input.readUTF();
         Class<? extends Matcher> matcherImplClass = (Class<? extends Matcher>) input.readObject();
         return new JPAContinuousQueryCacheEventFilterConverter(jpaQuery, matcherImplClass);
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
