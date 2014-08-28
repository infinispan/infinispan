package org.infinispan.query.remote.indexing;

import org.apache.lucene.document.Document;
import org.hibernate.search.bridge.LuceneOptions;
import org.infinispan.commons.CacheException;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.TagHandler;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.JavaType;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.protostream.impl.WrappedMessageMarshaller;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
final class WrappedMessageTagHandler implements TagHandler {

   private final Document document;
   private final LuceneOptions luceneOptions;
   private final SerializationContext serCtx;

   private Descriptor messageDescriptor;
   private byte[] bytes;
   private Number numericValue;
   private String stringValue;

   public WrappedMessageTagHandler(Document document, LuceneOptions luceneOptions, SerializationContext serCtx) {
      this.document = document;
      this.luceneOptions = luceneOptions;
      this.serCtx = serCtx;
   }

   @Override
   public void onStart() {
   }

   @Override
   public void onTag(int fieldNumber, String fieldName, Type type, JavaType javaType, Object value) {
      switch (fieldNumber) {
         case WrappedMessageMarshaller.WRAPPED_BOOL:
            numericValue = Boolean.TRUE.equals(value) ? IndexingTagHandler.TRUE_INT : IndexingTagHandler.FALSE_INT;
            break;
         case WrappedMessageMarshaller.WRAPPED_BYTES:
         case WrappedMessageMarshaller.WRAPPED_STRING:
            stringValue = (String) value;
            break;
         case WrappedMessageMarshaller.WRAPPED_ENUM:
         case WrappedMessageMarshaller.WRAPPED_DOUBLE:
         case WrappedMessageMarshaller.WRAPPED_FLOAT:
         case WrappedMessageMarshaller.WRAPPED_INT64:
         case WrappedMessageMarshaller.WRAPPED_INT32:
         case WrappedMessageMarshaller.WRAPPED_FIXED64:
         case WrappedMessageMarshaller.WRAPPED_FIXED32:
         case WrappedMessageMarshaller.WRAPPED_UINT32:
         case WrappedMessageMarshaller.WRAPPED_SFIXED32:
         case WrappedMessageMarshaller.WRAPPED_SFIXED64:
         case WrappedMessageMarshaller.WRAPPED_SINT32:
         case WrappedMessageMarshaller.WRAPPED_SINT64:
            numericValue = (Number) value;
            break;
         case WrappedMessageMarshaller.WRAPPED_DESCRIPTOR_FULL_NAME:
            messageDescriptor = serCtx.getMessageDescriptor((String) value);
            break;
         case WrappedMessageMarshaller.WRAPPED_MESSAGE_BYTES:
            bytes = (byte[]) value;
            break;
         default:
            throw new IllegalStateException("Unexpected field : " + fieldNumber);
      }
   }

   @Override
   public void onStartNested(int fieldNumber, String fieldName, Descriptor messageDescriptor) {
      throw new IllegalStateException("No nested message is expected");
   }

   @Override
   public void onEndNested(int fieldNumber, String fieldName, Descriptor messageDescriptor) {
      throw new IllegalStateException("No nested message is expected");
   }

   @Override
   public void onEnd() {
      if (bytes != null) {
         if (messageDescriptor == null) {
            throw new IllegalStateException("Descriptor name is missing");
         }
         IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         // if the message definition is not annotated at all we consider all fields indexed and stored, just to be backwards compatible
         if (indexingMetadata == null || indexingMetadata.isIndexed()) {
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
