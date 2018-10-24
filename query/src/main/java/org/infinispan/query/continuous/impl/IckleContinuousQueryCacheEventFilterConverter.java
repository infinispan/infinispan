package org.infinispan.query.continuous.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.dsl.embedded.impl.QueryCache;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public class IckleContinuousQueryCacheEventFilterConverter<K, V, C> extends AbstractCacheEventFilterConverter<K, V, C>
      implements IndexedFilter<K, V, C> {

   /**
    * The Ickle query to execute.
    */
   protected final String queryString;

   protected final Map<String, Object> namedParameters;

   /**
    * The implementation class of the Matcher component to lookup and use.
    */
   protected Class<? extends Matcher> matcherImplClass;

   /**
    * Optional cache for query objects.
    */
   protected QueryCache queryCache;

   /**
    * The Matcher, acquired via dependency injection.
    */
   protected Matcher matcher;

   /**
    * The ObjectFilter is created lazily.
    */
   protected ObjectFilter objectFilter;

   public IckleContinuousQueryCacheEventFilterConverter(String queryString, Map<String, Object> namedParameters, Class<? extends Matcher> matcherImplClass) {
      if (queryString == null || matcherImplClass == null) {
         throw new IllegalArgumentException("Arguments cannot be null");
      }
      this.queryString = queryString;
      this.namedParameters = namedParameters;
      this.matcherImplClass = matcherImplClass;
   }

   public Matcher getMatcher() {
      return matcher;
   }

   public String getQueryString() {
      return queryString;
   }

   public Map<String, Object> getNamedParameters() {
      return namedParameters;
   }

   /**
    * Acquires a Matcher instance from the ComponentRegistry of the given Cache object.
    */
   @Inject
   protected void injectDependencies(Cache cache) {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      queryCache = componentRegistry.getComponent(QueryCache.class);
      matcher = componentRegistry.getComponent(matcherImplClass);
      if (matcher == null) {
         throw new CacheException("Expected component not found in registry: " + matcherImplClass.getName());
      }
   }

   protected ObjectFilter getObjectFilter() {
      if (objectFilter == null) {
         objectFilter = queryCache != null
               ? queryCache.get(queryString, null, matcherImplClass, (qs, accumulators) -> matcher.getObjectFilter(qs))
               : matcher.getObjectFilter(queryString);
      }
      return namedParameters != null ? objectFilter.withParameters(namedParameters) : objectFilter;
   }

   @Override
   public C filterAndConvert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      if (eventType.isExpired()) {
         oldValue = newValue;   // expired events have the expired value as newValue
         newValue = null;
      }

      ObjectFilter objectFilter = getObjectFilter();
      ObjectFilter.FilterResult f1 = oldValue == null ? null : objectFilter.filter(oldValue);
      ObjectFilter.FilterResult f2 = newValue == null ? null : objectFilter.filter(newValue);
      if (f1 == null) {
         if (f2 != null) {
            // result joining
            return (C) new ContinuousQueryResult<>(ContinuousQueryResult.ResultType.JOINING, f2.getProjection() == null ? newValue : null, f2.getProjection());
         }
      } else {
         if (f2 != null) {
            // result updated
            return (C) new ContinuousQueryResult<>(ContinuousQueryResult.ResultType.UPDATED, f2.getProjection() == null ? newValue : null, f2.getProjection());
         } else {
            // result leaving
            return (C) new ContinuousQueryResult<V>(ContinuousQueryResult.ResultType.LEAVING, null, null);
         }
      }

      return null;
   }

   @Override
   public String toString() {
      return "IckleContinuousQueryCacheEventFilterConverter{queryString='" + queryString + "'}";
   }

   public static final class Externalizer extends AbstractExternalizer<IckleContinuousQueryCacheEventFilterConverter> {

      @Override
      public void writeObject(ObjectOutput output, IckleContinuousQueryCacheEventFilterConverter filterAndConverter) throws IOException {
         output.writeUTF(filterAndConverter.queryString);
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
      public IckleContinuousQueryCacheEventFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String queryString = input.readUTF();
         int paramsSize = UnsignedNumeric.readUnsignedInt(input);
         Map<String, Object> namedParameters = null;
         if (paramsSize != 0) {
            namedParameters = new HashMap<>(paramsSize);
            for (int i = 0; i < paramsSize; i++) {
               String paramName = input.readUTF();
               Object paramValue = input.readObject();
               namedParameters.put(paramName, paramValue);
            }
         }
         Class<? extends Matcher> matcherImplClass = (Class<? extends Matcher>) input.readObject();
         return new IckleContinuousQueryCacheEventFilterConverter(queryString, namedParameters, matcherImplClass);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.ICKLE_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER;
      }

      @Override
      public Set<Class<? extends IckleContinuousQueryCacheEventFilterConverter>> getTypeClasses() {
         return Collections.singleton(IckleContinuousQueryCacheEventFilterConverter.class);
      }
   }
}
