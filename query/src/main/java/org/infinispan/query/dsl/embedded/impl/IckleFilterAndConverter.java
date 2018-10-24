package org.infinispan.query.dsl.embedded.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.metadata.Metadata;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.FilterResultImpl;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

/**
 * A filter implementation that is both a KeyValueFilter and a converter. The implementation relies on the Matcher and a
 * Ickle query string.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public class IckleFilterAndConverter<K, V> extends AbstractKeyValueFilterConverter<K, V, ObjectFilter.FilterResult> implements Function<Map.Entry<K, V>, ObjectFilter.FilterResult> {

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

   /**
    * Acquires a Matcher instance from the ComponentRegistry of the given Cache object.
    */
   @Inject
   protected void injectDependencies(Cache cache) {
      this.queryCache = cache.getCacheManager().getGlobalComponentRegistry().getComponent(QueryCache.class);
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      matcher = componentRegistry.getComponent(matcherImplClass);
      if (matcher == null) {
         throw new CacheException("Expected component not found in registry: " + matcherImplClass.getName());
      }
   }

   public ObjectFilter getObjectFilter() {
      if (objectFilter == null) {
         objectFilter = queryCache != null
               ? queryCache.get(queryString, null, matcherImplClass, (qs, accumulators) -> matcher.getObjectFilter(qs))
               : matcher.getObjectFilter(queryString);
      }
      return namedParameters != null ? objectFilter.withParameters(namedParameters) : objectFilter;
   }

   public String getQueryString() {
      return queryString;
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
      return getObjectFilter().filter(value);
   }

   @Override
   public ObjectFilter.FilterResult apply(Map.Entry<K, V> cacheEntry) {
      return filterAndConvert(cacheEntry.getKey(), cacheEntry.getValue(), null);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{queryString='" + queryString + "'}";
   }

   public static final class IckleFilterAndConverterExternalizer extends AbstractExternalizer<IckleFilterAndConverter> {

      @Override
      public void writeObject(ObjectOutput output, IckleFilterAndConverter filterAndConverter) throws IOException {
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
      public IckleFilterAndConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
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
         return new IckleFilterAndConverter(queryString, namedParameters, matcherImplClass);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.ICKLE_FILTER_AND_CONVERTER;
      }

      @Override
      public Set<Class<? extends IckleFilterAndConverter>> getTypeClasses() {
         return Collections.singleton(IckleFilterAndConverter.class);
      }
   }

   public static final class FilterResultExternalizer extends AbstractExternalizer<FilterResultImpl> {

      @Override
      public void writeObject(ObjectOutput output, FilterResultImpl filterResult) throws IOException {
         if (filterResult.getProjection() != null) {
            // skip serializing the instance if there is a projection
            output.writeObject(null);
            output.writeObject(filterResult.getProjection());
         } else {
            output.writeObject(filterResult.getInstance());
         }
         output.writeObject(filterResult.getSortProjection());
      }

      @Override
      public FilterResultImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object instance = input.readObject();
         Object[] projection = instance == null ? (Object[]) input.readObject() : null;
         Comparable[] sortProjection = (Comparable[]) input.readObject();
         return new FilterResultImpl(instance, projection, sortProjection);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.ICKLE_FILTER_RESULT;
      }

      @Override
      public Set<Class<? extends FilterResultImpl>> getTypeClasses() {
         return Collections.singleton(FilterResultImpl.class);
      }
   }
}
