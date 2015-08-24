package org.infinispan.query.remote.impl.filter;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.metadata.Metadata;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.query.dsl.embedded.impl.JPAFilterAndConverter;
import org.infinispan.query.remote.impl.ExternalizerIds;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * A subclass of JPAFilterAndConverter that is able to deal with binary values wrapped in a ProtobufValueWrapper.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
public final class JPAProtobufFilterAndConverter<Object> extends JPAFilterAndConverter<Object, Object> {

   private boolean usesValueWrapper;

   @Override
   public void injectDependencies(Cache cache) {
      super.injectDependencies(cache);
      Configuration cfg = cache.getCacheConfiguration();
      usesValueWrapper = cfg.indexing().index().isEnabled() && !cfg.compatibility().enabled();
   }

   public JPAProtobufFilterAndConverter(String jpaQuery) {
      super(jpaQuery, ProtobufMatcher.class);
   }

   @Override
   public ObjectFilter.FilterResult filterAndConvert(Object key, Object value, Metadata metadata) {
      if (value == null) {
         // this is a 'pre' invocation, ignore it
         return null;
      }
      if (usesValueWrapper) {
         value = (Object) ((ProtobufValueWrapper) value).getBinary();
      }
      return getObjectFilter().filter(value);
   }

   @Override
   public String toString() {
      return "JPAProtobufFilterAndConverter{jpaQuery='" + getJPAQuery() + "'}";
   }

   public static final class Externalizer extends AbstractExternalizer<JPAProtobufFilterAndConverter> {

      @Override
      public void writeObject(ObjectOutput output, JPAProtobufFilterAndConverter filterAndConverter) throws IOException {
         output.writeUTF(filterAndConverter.getJPAQuery());
      }

      @Override
      public JPAProtobufFilterAndConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new JPAProtobufFilterAndConverter(input.readUTF());
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JPA_PROTOBUF_FILTER_AND_CONVERTER;
      }

      @Override
      public Set<Class<? extends JPAProtobufFilterAndConverter>> getTypeClasses() {
         return Collections.<Class<? extends JPAProtobufFilterAndConverter>>singleton(JPAProtobufFilterAndConverter.class);
      }
   }
}
