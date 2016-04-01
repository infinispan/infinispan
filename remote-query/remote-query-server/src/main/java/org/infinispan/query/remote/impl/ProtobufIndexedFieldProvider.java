package org.infinispan.query.remote.impl;

import org.infinispan.objectfilter.impl.syntax.BooleShannonExpansion;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.JavaType;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;

/**
 * Tests if a field is indexed by examining the Protobuf metadata.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
final class ProtobufIndexedFieldProvider implements BooleShannonExpansion.IndexedFieldProvider {

   private final Descriptor messageDescriptor;

   public ProtobufIndexedFieldProvider(Descriptor messageDescriptor) {
      this.messageDescriptor = messageDescriptor;
   }

   @Override
   public boolean isIndexed(String[] propertyPath) {
      Descriptor md = messageDescriptor;
      int i = 0;
      for (String p : propertyPath) {
         i++;
         FieldDescriptor field = md.findFieldByName(p);
         if (field == null) {
            break;
         }
         if (field.getJavaType() == JavaType.MESSAGE) {
            md = field.getMessageType();
         } else {
            if (i == propertyPath.length) {
               IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
               return indexingMetadata == null || indexingMetadata.isFieldIndexed(field.getNumber());
            } else {
               break;
            }
         }
      }
      return false;
   }

   @Override
   public boolean isStored(String[] propertyPath) {
      Descriptor md = messageDescriptor;
      int i = 0;
      for (String p : propertyPath) {
         i++;
         FieldDescriptor field = md.findFieldByName(p);
         if (field == null) {
            break;
         }
         if (field.getJavaType() == JavaType.MESSAGE) {
            md = field.getMessageType();
         } else {
            if (i == propertyPath.length) {
               IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
               return indexingMetadata == null || indexingMetadata.isFieldStored(field.getNumber());
            } else {
               break;
            }
         }
      }
      return false;
   }
}
