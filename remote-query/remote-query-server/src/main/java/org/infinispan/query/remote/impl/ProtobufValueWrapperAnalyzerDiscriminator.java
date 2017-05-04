package org.infinispan.query.remote.impl;

import org.hibernate.search.analyzer.Discriminator;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.logging.Log;
import org.infinispan.query.remote.impl.indexing.FieldMapping;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
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
      if (entity instanceof ProtobufValueWrapper) {
         ProtobufValueWrapper wrapper = (ProtobufValueWrapper) entity;
         Descriptor messageDescriptor = wrapper.getMessageDescriptor();
         if (messageDescriptor != null) {
            return getAnalyzerForField(messageDescriptor, field);
         }
      }
      return null;
   }

   private String getAnalyzerForField(Descriptor messageDescriptor, String fieldName) {
      IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
      if (indexingMetadata != null) {
         FieldMapping fieldMapping = indexingMetadata.getFieldMapping(fieldName);
         if (fieldMapping != null && fieldMapping.analyze()) {
            String analyzerName = fieldMapping.analyzer();
            if (analyzerName == null || analyzerName.isEmpty()) {
               analyzerName = indexingMetadata.analyzer();
            }
            if (analyzerName != null && !analyzerName.isEmpty()) {
               if (trace) {
                  log.tracef("Using analyzer %s for field %s of type %s", analyzerName, fieldName, messageDescriptor.getFullName());
               }
               return analyzerName;
            }
         }
      }
      return null;
   }
}
