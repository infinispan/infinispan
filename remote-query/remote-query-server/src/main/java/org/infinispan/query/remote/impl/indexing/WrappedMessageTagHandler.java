package org.infinispan.query.remote.impl.indexing;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.hibernate.search.bridge.LuceneOptions;
import org.infinispan.commons.CacheException;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.TagHandler;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.GenericDescriptor;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
final class WrappedMessageTagHandler implements TagHandler {

   private final ProtobufValueWrapper valueWrapper;
   private final Document document;
   private final LuceneOptions luceneOptions;
   private final SerializationContext serCtx;

   private Descriptor messageDescriptor;
   private byte[] bytes;
   private Number numericValue;
   private String stringValue;

   WrappedMessageTagHandler(ProtobufValueWrapper valueWrapper, Document document, LuceneOptions luceneOptions, SerializationContext serCtx) {
      this.valueWrapper = valueWrapper;
      this.document = document;
      this.luceneOptions = luceneOptions;
      this.serCtx = serCtx;
   }

   @Override
   public void onStart(GenericDescriptor descriptor) {
   }

   @Override
   public void onTag(int fieldNumber, FieldDescriptor fieldDescriptor, Object value) {
      switch (fieldNumber) {
         case WrappedMessage.WRAPPED_BOOL:
            stringValue = value != null ? value.toString() : null;
            break;
         case WrappedMessage.WRAPPED_BYTES:
         case WrappedMessage.WRAPPED_STRING:
            stringValue = (String) value;
            break;
         case WrappedMessage.WRAPPED_ENUM:
         case WrappedMessage.WRAPPED_DOUBLE:
         case WrappedMessage.WRAPPED_FLOAT:
         case WrappedMessage.WRAPPED_INT64:
         case WrappedMessage.WRAPPED_INT32:
         case WrappedMessage.WRAPPED_FIXED64:
         case WrappedMessage.WRAPPED_FIXED32:
         case WrappedMessage.WRAPPED_UINT32:
         case WrappedMessage.WRAPPED_SFIXED32:
         case WrappedMessage.WRAPPED_SFIXED64:
         case WrappedMessage.WRAPPED_SINT32:
         case WrappedMessage.WRAPPED_SINT64:
            numericValue = (Number) value;
            break;
         case WrappedMessage.WRAPPED_DESCRIPTOR_FULL_NAME:
            messageDescriptor = serCtx.getMessageDescriptor((String) value);
            break;
         case WrappedMessage.WRAPPED_DESCRIPTOR_ID:
            String typeName = serCtx.getTypeNameById((Integer) value);
            messageDescriptor = serCtx.getMessageDescriptor(typeName);
            break;
         case WrappedMessage.WRAPPED_MESSAGE:
            bytes = (byte[]) value;
            break;
         default:
            throw new IllegalStateException("Unexpected field : " + fieldNumber);
      }
   }

   @Override
   public void onStartNested(int fieldNumber, FieldDescriptor fieldDescriptor) {
      throw new IllegalStateException("No nested message is expected");
   }

   @Override
   public void onEndNested(int fieldNumber, FieldDescriptor fieldDescriptor) {
      throw new IllegalStateException("No nested message is expected");
   }

   @Override
   public void onEnd() {
      if (bytes != null) {
         // it's a message, not a primitive value
         if (messageDescriptor == null) {
            throw new IllegalStateException("Type name/id is missing");
         }
         IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         // if the message definition is not annotated at all we consider all fields indexed and stored, just to be backwards compatible
         if (indexingMetadata == null || indexingMetadata.isIndexed()) {
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
