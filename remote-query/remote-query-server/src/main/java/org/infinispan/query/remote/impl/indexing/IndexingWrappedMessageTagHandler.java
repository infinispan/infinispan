package org.infinispan.query.remote.impl.indexing;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.hibernate.search.bridge.LuceneOptions;
import org.infinispan.commons.CacheException;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;

/**
 * Protostream tag handler for {@code org.infinispan.protostream.WrappedMessage} protobuf type defined in
 * message-wrapping.proto which also indexes the message.
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

      if (bytes != null) {
         IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         // if the message definition is not annotated at all we consider all fields indexed and stored (but not analyzed), just to be backwards compatible
         if (indexingMetadata == null && IndexingMetadata.isLegacyIndexingEnabled(messageDescriptor) || indexingMetadata != null && indexingMetadata.isIndexed()) {
            if (indexingMetadata == null) {
               log.legacyIndexingIsDeprecated(messageDescriptor.getFullName(), messageDescriptor.getFileDescriptor().getName());
            }
            valueWrapper.setMessageDescriptor(messageDescriptor);
            try {
               ProtobufParser.INSTANCE.parse(new IndexingTagHandler(messageDescriptor, document), messageDescriptor, bytes);
            } catch (IOException e) {
               throw new CacheException(e);
            }
         }
      } else if (numericValue != null) {
         //todo [anistor] how do we index a scalar value?
         luceneOptions.addNumericFieldToDocument("theValue", numericValue, document);
      } else if (stringValue != null) {
         luceneOptions.addFieldToDocument("theValue", stringValue, document);
      }
   }
}
