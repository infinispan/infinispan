package org.infinispan.query.remote.impl.indexing;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.hibernate.search.bridge.LuceneOptions;
import org.infinispan.commons.CacheException;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.jboss.logging.Logger;

/**
 * Protostream tag handler for {@code org.infinispan.protostream.WrappedMessage} protobuf type defined in
 * message-wrapping.proto which adds all message fields that need to be indexed to a Lucene Document after parsing.
 *
 * @author anistor@redhat.com
 * @since 9.3
 */
final class IndexingWrappedMessageTagHandler extends WrappedMessageTagHandler {

   private final Document document;
   private final LuceneOptions luceneOptions;

   IndexingWrappedMessageTagHandler(ProtobufValueWrapper valueWrapper, SerializationContext serCtx, Document document, LuceneOptions luceneOptions) {
      super(valueWrapper, serCtx);
      this.document = document;
      this.luceneOptions = luceneOptions;
   }

   @Override
   public void onEnd() {
      super.onEnd();

      // parsing of WrappedMessage completed, now index the inner message or scalar value

      if (messageBytes != null) {
         // we are dealing with a message
         Descriptor descriptor = valueWrapper.getMessageDescriptor();
         IndexingMetadata indexingMetadata = descriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         // if the message definition is not annotated at all we consider all fields indexed and stored (but not analyzed), just to be backwards compatible
         if (indexingMetadata == null && IndexingMetadata.isLegacyIndexingEnabled(descriptor) || indexingMetadata != null && indexingMetadata.isIndexed()) {
            if (indexingMetadata == null) {
               if (log.isEnabled(Logger.Level.WARN)) {
                  log.legacyIndexingIsDeprecated(descriptor.getFullName(), descriptor.getFileDescriptor().getName());
               }
            }
            try {
               ProtobufParser.INSTANCE.parse(new IndexingTagHandler(descriptor, document), descriptor, messageBytes);
            } catch (IOException e) {
               throw new CacheException(e);
            }
         }
      } else if (numericValue != null) {
         // we are dealing with a numeric scalar
         //todo [anistor] how do we index a scalar value?
         luceneOptions.addNumericFieldToDocument("theValue", numericValue, document);
      } else if (stringValue != null) {
         // we are dealing with a string
         luceneOptions.addFieldToDocument("theValue", stringValue, document);
      }
   }
}
