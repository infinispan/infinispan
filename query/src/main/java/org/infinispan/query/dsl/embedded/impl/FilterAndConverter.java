package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.FilterResultImpl;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * A filter implementation that is both a KeyValueFilter and a converter. The implementation relies on the Matcher and a
 * JPA query string.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public class FilterAndConverter<K, V> extends AbstractKeyValueFilterConverter<K, V, ObjectFilter.FilterResult> {

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

   public FilterAndConverter(String jpaQuery, Class<? extends Matcher> matcherImplClass) {
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
   void injectDependencies(Cache cache) {
      matcher = cache.getAdvancedCache().getComponentRegistry().getComponent(matcherImplClass);
      if (matcher == null) {
         throw new CacheException("Expected component implementation not found: " + matcherImplClass.getName());
      }
   }

   public ObjectFilter getObjectFilter() {
      if (objectFilter == null) {
         objectFilter = matcher.getObjectFilter(jpaQuery);
      }
      return objectFilter;
   }

   @Override
   public ObjectFilter.FilterResult filterAndConvert(K key, V value, Metadata metadata) {
      return getObjectFilter().filter(value);
   }

   @Override
   public String toString() {
      return "FilterAndConverter{" +
            "jpaQuery='" + jpaQuery + '\'' +
            '}';
   }

   public static final class FilterAndConverterExternalizer extends AbstractExternalizer<FilterAndConverter> {

      @Override
      public void writeObject(ObjectOutput output, FilterAndConverter filterAndConverter) throws IOException {
         output.writeUTF(filterAndConverter.jpaQuery);
         output.writeObject(filterAndConverter.matcherImplClass);
      }

      @Override
      public FilterAndConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String jpaQuery = input.readUTF();
         Class<? extends Matcher> matcherImplClass = (Class<? extends Matcher>) input.readObject();
         return new FilterAndConverter(jpaQuery, matcherImplClass);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.FILTER_AND_CONVERTER;
      }

      @Override
      public Set<Class<? extends FilterAndConverter>> getTypeClasses() {
         return Collections.<Class<? extends FilterAndConverter>>singleton(FilterAndConverter.class);
      }
   }

   public static final class FilterResultExternalizer extends AbstractExternalizer<FilterResultImpl> {

      @Override
      public void writeObject(ObjectOutput output, FilterResultImpl filterResult) throws IOException {
         output.writeObject(filterResult.getInstance());
         output.writeObject(filterResult.getProjection());
         output.writeObject(filterResult.getSortProjection());
      }

      @Override
      public FilterResultImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object instance = input.readObject();
         Object[] projection = (Object[]) input.readObject();
         Comparable[] sortProjection = (Comparable[]) input.readObject();
         return new FilterResultImpl(instance, projection, sortProjection);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.FILTER_RESULT;
      }

      @Override
      public Set<Class<? extends FilterResultImpl>> getTypeClasses() {
         return Collections.<Class<? extends FilterResultImpl>>singleton(FilterResultImpl.class);
      }
   }
}