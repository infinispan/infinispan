package org.infinispan.query.remote.impl.indexing;

import org.hibernate.search.analyzer.Discriminator;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class ProtobufValueWrapperAnalyzerDiscriminator implements Discriminator {

   private static final Log log = LogFactory.getLog(ProtobufValueWrapperAnalyzerDiscriminator.class, Log.class);

   private static final boolean trace = log.isTraceEnabled();

   @Override
   public String getAnalyzerDefinitionName(Object value, Object entity, String field) {
      if (field == null) return null;

      String[] fieldPath = field.split("\\.");
      if (entity instanceof ProtobufValueWrapper) {
         ProtobufValueWrapper wrapper = (ProtobufValueWrapper) entity;
         Descriptor messageDescriptor = wrapper.getMessageDescriptor();
         if (messageDescriptor != null) {
            return getAnalyzerForField(messageDescriptor, fieldPath);
         } else {
            // this is either a scalar value (not indexed, why are we asked for analyzer -> bug)
            // or this entry was not run through ProtobufValueWrapperSearchWorkCreator (bug again)
            throw new IllegalStateException("Message descriptor not initialized for " + wrapper);
         }
      }
      return null;
   }

   private FieldDescriptor getFieldDescriptor(Descriptor messageDescriptor, String[] propertyPath) {
      FieldDescriptor fd = null;
      for (int i = 0; i < propertyPath.length; i++) {
         String name = propertyPath[i];
         fd = messageDescriptor.findFieldByName(name);
         if (fd == null) return null;
         IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata != null && !indexingMetadata.isFieldIndexed(name)) {
            return null;
         }
         if (i < propertyPath.length - 1) {
            messageDescriptor = fd.getMessageType();
         }
      }
      return fd;
   }

   private String getAnalyzerForField(Descriptor messageDescriptor, String[] fieldPath) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(messageDescriptor, fieldPath);
      if (fieldDescriptor != null) {
         IndexingMetadata indexingMetadata = fieldDescriptor.getContainingMessage().getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata != null) {
            FieldMapping fieldMapping = indexingMetadata.getFieldMapping(fieldDescriptor.getName());
            if (fieldMapping != null && fieldMapping.analyze()) {
               String analyzerName = fieldMapping.analyzer();
               if (analyzerName == null || analyzerName.isEmpty()) {
                  analyzerName = indexingMetadata.analyzer();
               }
               if (analyzerName != null && !analyzerName.isEmpty()) {
                  if (trace) {
                     log.tracef("Using analyzer %s for field %s of type %s", analyzerName, fieldPath, messageDescriptor.getFullName());
                  }
                  return analyzerName;
               }
            }
         }
      }
      return null;
   }
}
