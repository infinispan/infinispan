package org.infinispan.query.core.impl.continuous;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.core.impl.QueryCache;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
@ProtoTypeId(ProtoStreamTypeIds.ICKLE_CONTINOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER)
@Scope(Scopes.NONE)
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

   protected String cacheName;

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

   @ProtoFactory
   protected IckleContinuousQueryCacheEventFilterConverter(String queryString, MarshallableMap<String, Object> wrappedNamedParameters,
                                                 Class<? extends Matcher> matcherImplClass) {
      this(queryString, MarshallableMap.unwrap(wrappedNamedParameters), matcherImplClass);
   }

   @ProtoField(1)
   public String getQueryString() {
      return queryString;
   }

   @ProtoField(value = 2, name = "namedParameters")
   public MarshallableMap<String, Object> getWrappedNamedParameters() {
      return MarshallableMap.create(namedParameters);
   }

   @ProtoField(3)
   public Class<? extends Matcher> getMatcherImplClass() {
      return matcherImplClass;
   }

   public Matcher getMatcher() {
      return matcher;
   }

   public Map<String, Object> getNamedParameters() {
      return namedParameters;
   }

   /**
    * Acquires a Matcher instance from the ComponentRegistry of the given Cache object.
    */
   @Inject
   protected void injectDependencies(Cache<?, ?> cache) {
      cacheName = cache.getName();
      ComponentRegistry componentRegistry =  ComponentRegistry.of(cache);
      queryCache = componentRegistry.getComponent(QueryCache.class);
      matcher = componentRegistry.getComponent(matcherImplClass);
      if (matcher == null) {
         throw new CacheException("Expected component not found in registry: " + matcherImplClass.getName());
      }
   }

   protected ObjectFilter getObjectFilter() {
      if (objectFilter == null) {
         objectFilter = queryCache != null
               ? queryCache.get(cacheName, queryString, null, matcherImplClass, (qs, accumulators) -> matcher.getObjectFilter(qs))
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
      ObjectFilter.FilterResult f1 = oldValue == null ? null : objectFilter.filter(key, oldValue, oldMetadata);
      ObjectFilter.FilterResult f2 = newValue == null ? null : objectFilter.filter(key, newValue, newMetadata);
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
            return (C) new ContinuousQueryResult<V>(ContinuousQueryResult.ResultType.LEAVING, (V) null, null);
         }
      }

      return null;
   }

   @Override
   public String toString() {
      return "IckleContinuousQueryCacheEventFilterConverter{queryString='" + queryString + "'}";
   }
}
