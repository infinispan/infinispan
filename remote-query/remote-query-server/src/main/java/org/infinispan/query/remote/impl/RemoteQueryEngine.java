package org.infinispan.query.remote.impl;

import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.remote.impl.filter.IckleProtobufFilterAndConverter;
import org.infinispan.util.function.SerializableFunction;

/**
 * Same as ObjectRemoteQueryEngine but able to deal with protobuf payloads.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
final class RemoteQueryEngine extends ObjectRemoteQueryEngine {

   private static final SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> queryEngineProvider = c -> c.getComponentRegistry().getComponent(RemoteQueryManager.class).getQueryEngine(c);

   RemoteQueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed) {
      super(isIndexed ? cache.withStorageMediaType().withWrapping(ByteArrayWrapper.class, ProtobufWrapper.class) : cache.withStorageMediaType(), isIndexed, ProtobufMatcher.class);
   }

   //todo [anistor] null markers and boolean conversions seem to be a thing of the past, might not be needed anymore after the HS6 migration. need to cleanup here!
   @Override
   protected RowProcessor makeProjectionProcessor(Class<?>[] projectedTypes, Object[] projectedNullMarkers) {
      if (projectedNullMarkers != null) {
         for (Object projectedNullMarker : projectedNullMarkers) {
            if (projectedNullMarker != null) {
               return row -> {
                  for (int i = 0; i < projectedNullMarkers.length; i++) {
                     if (row[i] != null && row[i].equals(projectedNullMarkers[i])) {
                        row[i] = null;
                     }
                  }
                  return row;
               };
            }
         }
      }
      return null;
   }

   @Override
   protected SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> getQueryEngineProvider() {
      return queryEngineProvider;
   }

   @Override
   protected IckleFilterAndConverter createFilter(String queryString, Map<String, Object> namedParameters) {
      return isIndexed ? new IckleProtobufFilterAndConverter(queryString, namedParameters) :
            super.createFilter(queryString, namedParameters);
   }

   @Override
   protected Class<?> getTargetedClass(IckleParsingResult<?> parsingResult) {
      return byte[].class;
   }

   @Override
   protected String getTargetedNamedType(IckleParsingResult<?> parsingResult) {
      Descriptor targetEntityMetadata = (Descriptor) parsingResult.getTargetEntityMetadata();
      return targetEntityMetadata.getFullName();
   }
}
