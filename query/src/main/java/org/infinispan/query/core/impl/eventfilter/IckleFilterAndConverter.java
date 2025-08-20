package org.infinispan.query.core.impl.eventfilter;

import java.util.Map;
import java.util.function.Function;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.metadata.Metadata;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.core.impl.QueryCache;

/**
 * A filter implementation that is both a KeyValueFilter and a converter. The implementation relies on the Matcher and a
 * Ickle query string.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.ICKLE_FILTER_AND_CONVERTER)
@Scope(Scopes.NONE)
public class IckleFilterAndConverter<K, V> extends AbstractKeyValueFilterConverter<K, V, ObjectFilter.FilterResult> implements Function<Map.Entry<K, V>, ObjectFilter.FilterResult> {

   private String cacheName;

   /**
    * Optional cache for query objects.
    */
   private QueryCache queryCache;

   /**
    * The Ickle query to execute.
    */
   private final String queryString;

   private final Map<String, Object> namedParameters;

   /**
    * The implementation class of the Matcher component to lookup and use.
    */
   protected Class<? extends Matcher> matcherImplClass;

   /**
    * The Matcher, acquired via dependency injection.
    */
   private Matcher matcher;

   /**
    * The ObjectFilter is created lazily.
    */
   private ObjectFilter objectFilter;

   public IckleFilterAndConverter(String queryString, Map<String, Object> namedParameters, Class<? extends Matcher> matcherImplClass) {
      if (queryString == null || matcherImplClass == null) {
         throw new IllegalArgumentException("Arguments cannot be null");
      }
      this.queryString = queryString;
      this.namedParameters = namedParameters;
      this.matcherImplClass = matcherImplClass;
   }

   @ProtoFactory
   protected IckleFilterAndConverter(String queryString, MarshallableMap<String, Object> wrappedNamedParameters, Class<? extends Matcher> matcherImplClass) {
      this(queryString, MarshallableMap.unwrap(wrappedNamedParameters), matcherImplClass);
   }

   @ProtoField(1)
   public String getQueryString() {
      return queryString;
   }

   @ProtoField(2)
   public MarshallableMap<String, Object> getWrappedNamedParameters() {
      return MarshallableMap.create(namedParameters);
   }

   @ProtoField(3)
   public Class<? extends Matcher> getMatcherImplClass() {
      return matcherImplClass;
   }

   /**
    * Acquires a Matcher instance from the ComponentRegistry of the given Cache object.
    */
   @Inject
   protected void injectDependencies(ComponentRegistry componentRegistry, QueryCache queryCache) {
      this.queryCache = queryCache;
      cacheName = componentRegistry.getCache().wired().getName();
      matcher = componentRegistry.getComponent(matcherImplClass);
      if (matcher == null) {
         throw new CacheException("Expected component not found in registry: " + matcherImplClass.getName());
      }
   }

   public ObjectFilter getObjectFilter() {
      if (objectFilter == null) {
         objectFilter = queryCache != null
               ? queryCache.get(cacheName, queryString, null, matcherImplClass, (qs, accumulators) -> matcher.getObjectFilter(qs))
               : matcher.getObjectFilter(queryString);
      }
      return namedParameters != null ? objectFilter.withParameters(namedParameters) : objectFilter;
   }


   public Map<String, Object> getNamedParameters() {
      return namedParameters;
   }

   public Matcher getMatcher() {
      return matcher;
   }

   @Override
   public ObjectFilter.FilterResult filterAndConvert(K key, V value, Metadata metadata) {
      if (value == null) {
         return null;
      }
      return getObjectFilter().filter(key, value, metadata);
   }

   @Override
   public ObjectFilter.FilterResult apply(Map.Entry<K, V> cacheEntry) {
      return filterAndConvert(cacheEntry.getKey(), cacheEntry.getValue(), null);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{queryString='" + queryString + "'}";
   }
}
