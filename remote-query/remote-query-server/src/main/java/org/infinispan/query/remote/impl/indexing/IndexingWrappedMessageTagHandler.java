package org.infinispan.query.remote.impl.indexing;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.hibernate.search.bridge.LuceneOptions;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.remote.impl.logging.Log;

/**
 * Protostream tag handler for {@code org.infinispan.protostream.WrappedMessage} protobuf type defined in
 * message-wrapping.proto which adds all message fields that need to be indexed to a Lucene Document after parsing.
 *
 * @author anistor@redhat.com
 * @since 9.3
 */
final class IndexingWrappedMessageTagHandler extends WrappedMessageTagHandler {

   private static final Log log = LogFactory.getLog(IndexingWrappedMessageTagHandler.class, Log.class);

   private final Document document;
   private final LuceneOptions luceneOptions;
   private final Set<String> indexedTypes;

   IndexingWrappedMessageTagHandler(ProtobufValueWrapper valueWrapper, SerializationContext serCtx, Set<String> indexedTypes, Document document, LuceneOptions luceneOptions) {
      super(valueWrapper, serCtx);
      this.document = document;
      this.luceneOptions = luceneOptions;
      this.indexedTypes = indexedTypes;
   }

   @Override
   public void onEnd() {
      super.onEnd();

      // parsing of WrappedMessage completed, now index the inner message or the scalar value

      if (messageBytes != null) {
         // we are dealing with a message
         Descriptor descriptor = valueWrapper.getMessageDescriptor();
         IndexingMetadata indexingMetadata = descriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);

         // not all message types are annotated for indexing
         if (indexingMetadata != null && indexingMetadata.isIndexed()) {
            // must ensure it's one of the declared indexed types
            if (!indexedTypes.contains(descriptor.getFullName())) {
               throw new CacheConfigurationException("Type " + descriptor.getFullName() + " was not declared as an indexed entity");
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
         luceneOptions.addNumericFieldToDocument("$$value$$", numericValue, document);
      } else if (stringValue != null) {
         // we are dealing with a string
         luceneOptions.addFieldToDocument("$$value$$", stringValue, document);
      }
   }
}
