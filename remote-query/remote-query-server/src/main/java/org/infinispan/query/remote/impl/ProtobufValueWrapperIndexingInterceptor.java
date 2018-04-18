package org.infinispan.query.remote.impl;

import org.hibernate.search.indexes.interceptor.EntityIndexingInterceptor;
import org.hibernate.search.indexes.interceptor.IndexingOverride;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;

/**
 * Hibernate Search interceptor for conditional indexing of protobuf message types based on the value of the
 * {@literal @}Indexed protobuf documentation annotation.
 *
 * @author anistor@redhat.com
 * @since 9.3
 */
public final class ProtobufValueWrapperIndexingInterceptor implements EntityIndexingInterceptor<ProtobufValueWrapper> {

   @Override
   public IndexingOverride onAdd(ProtobufValueWrapper entity) {
      return isIndexed(entity) ? IndexingOverride.APPLY_DEFAULT : IndexingOverride.SKIP;
   }

   @Override
   public IndexingOverride onUpdate(ProtobufValueWrapper entity) {
      return isIndexed(entity) ? IndexingOverride.APPLY_DEFAULT : IndexingOverride.SKIP;
   }

   @Override
   public IndexingOverride onDelete(ProtobufValueWrapper entity) {
      return isIndexed(entity) ? IndexingOverride.APPLY_DEFAULT : IndexingOverride.SKIP;
   }

   @Override
   public IndexingOverride onCollectionUpdate(ProtobufValueWrapper entity) {
      return isIndexed(entity) ? IndexingOverride.APPLY_DEFAULT : IndexingOverride.SKIP;
   }

   private boolean isIndexed(ProtobufValueWrapper entity) {
      Descriptor messageDescriptor = entity.getMessageDescriptor();
      // lack of message descriptor means scalar type, which we do not currently index!
      if (messageDescriptor == null) {
         return false;
      }
      IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
      return indexingMetadata == null && IndexingMetadata.isLegacyIndexingEnabled(messageDescriptor)
            || indexingMetadata != null && indexingMetadata.isIndexed();
   }
}
