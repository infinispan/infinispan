package org.infinispan.query.remote.impl.filter;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A subclass of JPAFilterAndConverter that is able to deal with binary values wrapped in a ProtobufValueWrapper.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
public final class JPAProtobufFilterAndConverter extends JPAFilterAndConverter<Object, Object> {

   private boolean usesValueWrapper;

   public JPAProtobufFilterAndConverter(String jpaQuery, Map<String, Object> namedParameters) {
      super(jpaQuery, namedParameters, ProtobufMatcher.class);
   }

   @Inject
   protected void injectDependencies(Configuration cfg) {
      usesValueWrapper = cfg.indexing().index().isEnabled() && !cfg.compatibility().enabled();
   }

   @Override
   public ObjectFilter.FilterResult filterAndConvert(Object key, Object value, Metadata metadata) {
      if (value == null) {
         return null;
      }
      if (usesValueWrapper) {
         value = ((ProtobufValueWrapper) value).getBinary();
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
         Map<String, Object> namedParameters = filterAndConverter.getNamedParameters();
         if (namedParameters != null) {
            UnsignedNumeric.writeUnsignedInt(output, namedParameters.size());
            for (Map.Entry<String, Object> e : namedParameters.entrySet()) {
               output.writeUTF(e.getKey());
               output.writeObject(e.getValue());
            }
         } else {
            UnsignedNumeric.writeUnsignedInt(output, 0);
         }
      }

      @Override
      public JPAProtobufFilterAndConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
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
         return new JPAProtobufFilterAndConverter(jpaQuery, namedParameters);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JPA_PROTOBUF_FILTER_AND_CONVERTER;
      }

      @Override
      public Set<Class<? extends JPAProtobufFilterAndConverter>> getTypeClasses() {
         return Collections.singleton(JPAProtobufFilterAndConverter.class);
      }
   }
}
