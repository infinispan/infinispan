package org.infinispan.query.remote.impl;

import java.util.function.BiFunction;

import org.infinispan.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.JavaType;
import org.infinispan.query.remote.impl.indexing.FieldMapping;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;

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
   public boolean hasProperty(String[] propertyPath) {
      Descriptor md = messageDescriptor;
      int i = 0;
      for (String p : propertyPath) {
         i++;
         FieldDescriptor field = null;
         IndexingMetadata indexingMetadata = md.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata != null) {
            FieldMapping fieldMapping = indexingMetadata.getFieldMapping(p);
            if (fieldMapping != null) {
               field = fieldMapping.getFieldDescriptor();
            }
         }
         if (field == null) {
            field = md.findFieldByName(p);
         }
         if (field == null) {
            break;
         }
         if (field.getJavaType() == JavaType.MESSAGE) {
            md = field.getMessageType();
         } else {
            return i == propertyPath.length;
         }
      }
      return false;
   }

   @Override
   public boolean isIndexed(String[] propertyPath) {
      return getFlag(propertyPath, IndexingMetadata::isFieldIndexed);
   }

   @Override
   public boolean isAnalyzed(String[] propertyPath) {
      return getFlag(propertyPath, IndexingMetadata::isFieldAnalyzed);
   }

   @Override
   public boolean isStored(String[] propertyPath) {
      return getFlag(propertyPath, IndexingMetadata::isFieldStored);
   }

   @Override
   public boolean isSpatial(String[] propertyPath) {
      return getFlag(propertyPath, IndexingMetadata::isFieldSpatial);
   }

   @Override
   public Object getNullMarker(String[] propertyPath) {
      Descriptor md = messageDescriptor;
      int i = 0;
      for (String p : propertyPath) {
         i++;
         FieldDescriptor field = null;
         IndexingMetadata indexingMetadata = md.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata != null) {
            FieldMapping fieldMapping = indexingMetadata.getFieldMapping(p);
            if (fieldMapping != null) {
               if (i == propertyPath.length) {
                  return fieldMapping.indexNullAs();
               }
               field = fieldMapping.getFieldDescriptor();
            }
         }
         if (field == null) {
            field = md.findFieldByName(p);
         }
         if (i == propertyPath.length || field == null || field.getJavaType() != JavaType.MESSAGE) {
            break;
         }
         md = field.getMessageType();
      }
      return null;
   }

   private boolean getFlag(String[] propertyPath, BiFunction<IndexingMetadata, String, Boolean> metadataFun) {
      Descriptor md = messageDescriptor;
      int i = 0;
      for (String p : propertyPath) {
         i++;
         FieldDescriptor field = null;
         IndexingMetadata indexingMetadata = md.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata != null) {
            FieldMapping fieldMapping = indexingMetadata.getFieldMapping(p);
            if (fieldMapping != null) {
               if (!metadataFun.apply(indexingMetadata, p)) {
                  break;
               }
               if (i == propertyPath.length) {
                  return true;
               }
               field = fieldMapping.getFieldDescriptor();
            }
         }
         if (field == null) {
            field = md.findFieldByName(p);
         }
         if (field == null || field.getJavaType() != JavaType.MESSAGE) {
            break;
         }
         md = field.getMessageType();
      }
      return false;
   }
}
