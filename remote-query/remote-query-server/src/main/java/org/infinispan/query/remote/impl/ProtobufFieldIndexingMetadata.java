package org.infinispan.query.remote.impl;

import static org.infinispan.query.remote.impl.indexing.IndexingMetadata.findProcessedAnnotation;

import java.util.function.BiFunction;

import org.infinispan.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.JavaType;
import org.infinispan.query.remote.impl.indexing.FieldMapping;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;
import org.infinispan.query.remote.impl.mapping.reference.MessageReferenceProvider;

/**
 * Tests if a field is indexed by examining the Protobuf metadata.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
final class ProtobufFieldIndexingMetadata implements IndexedFieldProvider.FieldIndexingMetadata {

   private final Descriptor messageDescriptor;

   ProtobufFieldIndexingMetadata(Descriptor messageDescriptor) {
      if (messageDescriptor == null) {
         throw new IllegalArgumentException("argument cannot be null");
      }
      this.messageDescriptor = messageDescriptor;
   }

   @Override
   public boolean isIndexed(String[] propertyPath) {
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
   public boolean isSortable(String[] propertyPath) {
      return getFlag(propertyPath, IndexingMetadata::isFieldSortable);
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

   private boolean getFlag(String[] propertyPath, BiFunction<IndexingMetadata, String, Boolean> metadataFun) {
      Descriptor md = messageDescriptor;
      for (String p : propertyPath) {
         FieldDescriptor field = md.findFieldByName(p);
         if (field == null) {
            return false;
         }

         IndexingMetadata indexingMetadata = findProcessedAnnotation(md, IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata == null || !indexingMetadata.isIndexed()){
            return false;
         }

         if (field.getJavaType() == JavaType.MESSAGE &&
               !MessageReferenceProvider.COMMON_MESSAGE_TYPES.contains(field.getTypeName())) {
            FieldMapping embeddedMapping = indexingMetadata.getFieldMapping(p);
            if (embeddedMapping == null || !embeddedMapping.searchable()) {
               return false;
            }

            md = field.getMessageType();
            continue;
         }

         return metadataFun.apply(indexingMetadata, field.getName());
      }
      return false;
   }
}
