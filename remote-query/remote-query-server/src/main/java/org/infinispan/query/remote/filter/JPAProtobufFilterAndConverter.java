package org.infinispan.query.remote.filter;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.metadata.Metadata;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.query.dsl.embedded.impl.JPAFilterAndConverter;
import org.infinispan.query.remote.ExternalizerIds;
import org.infinispan.query.remote.indexing.ProtobufValueWrapper;

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
public final class JPAProtobufFilterAndConverter<K> extends JPAFilterAndConverter<K, ProtobufValueWrapper> {

   public JPAProtobufFilterAndConverter(String jpaQuery) {
      super(jpaQuery, ProtobufMatcher.class);
   }

   @Override
   public ObjectFilter.FilterResult filterAndConvert(K key, ProtobufValueWrapper value, Metadata metadata) {
      if (value == null) {
         // this is a 'pre' invocation, ignore it
         return null;
      }
      return getObjectFilter().filter(value.getBinary());
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
