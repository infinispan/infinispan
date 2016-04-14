package org.infinispan.query.remote.impl;

import org.infinispan.objectfilter.impl.syntax.BooleShannonExpansion;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.JavaType;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;

import java.util.function.BiFunction;

/**
 * Tests if a field is indexed by examining the Protobuf metadata.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
final class ProtobufIndexedFieldProvider implements BooleShannonExpansion.IndexedFieldProvider {

   private final Descriptor messageDescriptor;

   ProtobufIndexedFieldProvider(Descriptor messageDescriptor) {
      if (messageDescriptor == null) {
         throw new IllegalArgumentException("argument cannot be null");
      }
      this.messageDescriptor = messageDescriptor;
   }

   @Override
   public boolean isIndexed(String[] propertyPath) {
      return getMetadata(propertyPath, IndexingMetadata::isFieldIndexed);
   }

   @Override
   public boolean isStored(String[] propertyPath) {
      return getMetadata(propertyPath, IndexingMetadata::isFieldStored);
   }

   private boolean getMetadata(String[] propertyPath, BiFunction<IndexingMetadata, Integer, Boolean> metadataFun) {
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
               return indexingMetadata == null || metadataFun.apply(indexingMetadata, field.getNumber());
            } else {
               break;
            }
         }
      }
      return false;
   }
}
