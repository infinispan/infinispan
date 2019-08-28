package org.infinispan.query.remote.impl.filter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.query.core.impl.QueryCache;
import org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter;
import org.infinispan.query.remote.impl.ExternalizerIds;
import org.infinispan.query.remote.impl.RemoteQueryManager;

/**
 * A subclass of {@link IckleFilterAndConverter} that is able to deal with binary protobuf values wrapped in a
 * ProtobufValueWrapper.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
public final class IckleProtobufFilterAndConverter extends IckleFilterAndConverter<Object, Object> {

   public IckleProtobufFilterAndConverter(String queryString, Map<String, Object> namedParameters) {
      super(queryString, namedParameters, ProtobufMatcher.class);
   }

   @Override
   protected void injectDependencies(ComponentRegistry componentRegistry, QueryCache queryCache) {
      RemoteQueryManager remoteQueryManager = componentRegistry.getComponent(RemoteQueryManager.class);
      matcherImplClass = remoteQueryManager.getMatcherClass(MediaType.APPLICATION_PROTOSTREAM);
      super.injectDependencies(componentRegistry, queryCache);
   }

   public static final class Externalizer extends AbstractExternalizer<IckleProtobufFilterAndConverter> {

      @Override
      public void writeObject(ObjectOutput output, IckleProtobufFilterAndConverter filterAndConverter) throws IOException {
         output.writeUTF(filterAndConverter.getQueryString());
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
      public IckleProtobufFilterAndConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
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
         return new IckleProtobufFilterAndConverter(queryString, namedParameters);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.ICKLE_PROTOBUF_FILTER_AND_CONVERTER;
      }

      @Override
      public Set<Class<? extends IckleProtobufFilterAndConverter>> getTypeClasses() {
         return Collections.singleton(IckleProtobufFilterAndConverter.class);
      }
   }
}
