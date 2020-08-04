package org.infinispan.query.remote.impl;

import java.util.Arrays;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.impl.QueryDefinition;
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

   @Override
   protected RowProcessor makeProjectionProcessor(Class<?>[] projectedTypes, Object[] projectedNullMarkers) {
      // Protobuf's booleans are indexed as Strings, so we need to convert them.
      // Collect here the positions of all Boolean projections.
      int[] booleanPositions = new int[projectedTypes.length];
      int booleanColumnsNumber = 0;
      for (int i = 0; i < projectedTypes.length; i++) {
         if (projectedTypes[i] == Boolean.class) {
            booleanPositions[booleanColumnsNumber++] = i;
         }
      }
      boolean hasNullMarkers = false;
      if (projectedNullMarkers != null) {
         for (Object projectedNullMarker : projectedNullMarkers) {
            if (projectedNullMarker != null) {
               hasNullMarkers = true;
               break;
            }
         }
      }
      if (booleanColumnsNumber == 0 && !hasNullMarkers) {
         return null;
      }
      final boolean hasNullMarkers_ = hasNullMarkers;
      final int[] booleanColumns = booleanColumnsNumber < booleanPositions.length ? Arrays.copyOf(booleanPositions, booleanColumnsNumber) : booleanPositions;
      return row -> {
         if (hasNullMarkers_) {
            for (int i = 0; i < projectedNullMarkers.length; i++) {
               if (row[i] != null && row[i].equals(projectedNullMarkers[i])) {
                  row[i] = null;
               }
            }
         }
         return row;
      };
   }

   @Override
   protected QueryDefinition buildQueryDefinition(String q) {
      return new QueryDefinition(q, queryEngineProvider);
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
