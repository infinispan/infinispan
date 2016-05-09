package org.infinispan.query.dsl.embedded.impl;

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
import org.infinispan.util.KeyValuePair;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A filter implementation that is both a KeyValueFilter and a converter. The implementation relies on the Matcher and a
 * JPA query string.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public class JPAFilterAndConverter<K, V> extends AbstractKeyValueFilterConverter<K, V, ObjectFilter.FilterResult> {

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
   protected Class<? extends Matcher> matcherImplClass;

   /**
    * The Matcher, acquired via dependency injection.
    */
   private Matcher matcher;

   /**
    * The ObjectFilter is created lazily.
    */
   private ObjectFilter objectFilter;

   public JPAFilterAndConverter(String jpaQuery, Map<String, Object> namedParameters, Class<? extends Matcher> matcherImplClass) {
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
         if (queryCache != null) {
            KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<>(jpaQuery, matcherImplClass);
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
      return namedParameters != null ? objectFilter.withParameters(namedParameters) : objectFilter;
   }

   public String getJPAQuery() {
      return jpaQuery;
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
   public String toString() {
      return "JPAFilterAndConverter{jpaQuery='" + jpaQuery + "'}";
   }

   public static final class JPAFilterAndConverterExternalizer extends AbstractExternalizer<JPAFilterAndConverter> {

      @Override
      public void writeObject(ObjectOutput output, JPAFilterAndConverter filterAndConverter) throws IOException {
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
      public JPAFilterAndConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String jpaQuery = input.readUTF();
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
         return new JPAFilterAndConverter(jpaQuery, namedParameters, matcherImplClass);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JPA_FILTER_AND_CONVERTER;
      }

      @Override
      public Set<Class<? extends JPAFilterAndConverter>> getTypeClasses() {
         return Collections.singleton(JPAFilterAndConverter.class);
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
         return ExternalizerIds.JPA_FILTER_RESULT;
      }

      @Override
      public Set<Class<? extends FilterResultImpl>> getTypeClasses() {
         return Collections.singleton(FilterResultImpl.class);
      }
   }
}
