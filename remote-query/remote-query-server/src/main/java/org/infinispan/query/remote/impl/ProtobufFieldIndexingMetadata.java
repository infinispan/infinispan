package org.infinispan.query.remote.impl;

import static org.infinispan.query.remote.impl.indexing.IndexingMetadata.findProcessedAnnotation;

import java.util.Map;
import java.util.function.BiFunction;

import org.infinispan.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.GenericDescriptor;
import org.infinispan.protostream.descriptors.JavaType;
import org.infinispan.query.remote.impl.indexing.FieldMapping;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;
import org.infinispan.query.remote.impl.indexing.infinispan.IndexingMetadataHolder;
import org.infinispan.query.remote.impl.indexing.infinispan.InfinispanAnnotations;
import org.infinispan.query.remote.impl.mapping.reference.MessageReferenceProvider;

/**
 * Tests if a field is indexed by examining the Protobuf metadata.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
final class ProtobufFieldIndexingMetadata implements IndexedFieldProvider.FieldIndexingMetadata {

   private final Descriptor messageDescriptor;
   private final IndexingMetadata indexingMetadata;
   private final String keyProperty;
   private final Descriptor keyMessageDescriptor;

   ProtobufFieldIndexingMetadata(Descriptor messageDescriptor, Map<String, GenericDescriptor> genericDescriptors) {
      if (messageDescriptor == null) {
         throw new IllegalArgumentException("argument cannot be null");
      }
      this.messageDescriptor = messageDescriptor;
      indexingMetadata = findProcessedAnnotation(messageDescriptor, IndexingMetadata.INDEXED_ANNOTATION);
      if (indexingMetadata != null && indexingMetadata.indexingKey() != null) {
         keyProperty = indexingMetadata.indexingKey().fieldName();
         keyMessageDescriptor = (Descriptor) genericDescriptors.get(indexingMetadata.indexingKey().typeFullName());
      } else {
         keyProperty = null;
         keyMessageDescriptor = null;
      }
   }

   @Override
   public boolean isSearchable(String[] propertyPath) {
      return getFlag(propertyPath, IndexingMetadata::isFieldSearchable);
   }

   @Override
   public boolean isAnalyzed(String[] propertyPath) {
      return getFlag(propertyPath, IndexingMetadata::isFieldAnalyzed);
   }

   @Override
   public boolean isNormalized(String[] propertyPath) {
      return getFlag(propertyPath, IndexingMetadata::isFieldNormalized);
   }

   @Override
   public boolean isProjectable(String[] propertyPath) {
      return getFlag(propertyPath, IndexingMetadata::isFieldProjectable);
   }

   @Override
   public boolean isAggregable(String[] propertyPath) {
      return getFlag(propertyPath, IndexingMetadata::isFieldAggregable);
   }

   @Override
   public boolean isSortable(String[] propertyPath) {
      return getFlag(propertyPath, IndexingMetadata::isFieldSortable);
   }

   @Override
   public boolean isVector(String[] propertyPath) {
      return getFlag(propertyPath, IndexingMetadata::isVectorField);
   }

   @Override
   public Object getNullMarker(String[] propertyPath) {
      Descriptor md = messageDescriptor;
      int i = 0;
      for (String p : propertyPath) {
         i++;
         FieldDescriptor field = md.findFieldByName(p);
         if (field == null) {
            break;
         }
         if (i == propertyPath.length) {
            IndexingMetadata indexingMetadata = findProcessedAnnotation(md, IndexingMetadata.INDEXED_ANNOTATION);
            return indexingMetadata == null ? null : indexingMetadata.getNullMarker(field.getName());
         }
         if (field.getJavaType() != JavaType.MESSAGE) {
            break;
         }
         md = field.getMessageType();
      }
      return null;
   }

   @Override
   public Descriptor keyType(String property) {
      return (property.equals(keyProperty)) ? keyMessageDescriptor : null;
   }

   private boolean getFlag(String[] propertyPath, BiFunction<IndexingMetadata, String, Boolean> metadataFun) {
      Descriptor md = messageDescriptor;
      IndexingMetadataHolder indexingMetadataHolder = null;
      for (int i = 0; i < propertyPath.length; i++) {
         String property = propertyPath[i];
         FieldDescriptor field = md.findFieldByName(property);
         if (field == null) {
            if (i == 0) {
               md = keyType(property);
               if (md != null) {
                  continue;
               }
            }

            return false;
         }

         IndexingMetadata indexingMetadata = findProcessedAnnotation(md, IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata == null || !indexingMetadata.isIndexed()) {
            if (indexingMetadataHolder == null || indexingMetadataHolder.getIndexingMetadata() == null) {
               return false;
            }
            indexingMetadata = indexingMetadataHolder.getIndexingMetadata();
         }

         if (field.getJavaType() == JavaType.MESSAGE &&
               !MessageReferenceProvider.COMMON_MESSAGE_TYPES.contains(field.getTypeName())) {
            FieldMapping embeddedMapping = indexingMetadata.getFieldMapping(property);
            if (embeddedMapping == null || !embeddedMapping.searchable()) {
               return false;
            }

            md = field.getMessageType();
            indexingMetadataHolder = findProcessedAnnotation(field, InfinispanAnnotations.EMBEDDED_ANNOTATION);
            continue;
         }

         return metadataFun.apply(indexingMetadata, field.getName());
      }
      return false;
   }
}
