package org.infinispan.query.remote.impl.indexing;

import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.TagHandler;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.GenericDescriptor;

/**
 * Protostream tag handler for {@code org.infinispan.protostream.WrappedMessage} protobuf type defined in
 * message-wrapping.proto. This handler extracts the embedded value or message but does not parse the message, it just
 * discovers its type.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
class WrappedMessageTagHandler implements TagHandler {

   protected final ProtobufValueWrapper valueWrapper;
   protected final SerializationContext serCtx;

   protected GenericDescriptor descriptor;
   protected byte[] messageBytes;
   protected Number numericValue;
   protected String stringValue;

   WrappedMessageTagHandler(ProtobufValueWrapper valueWrapper, SerializationContext serCtx) {
      this.valueWrapper = valueWrapper;
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
         case WrappedMessage.WRAPPED_DESCRIPTOR_FULL_NAME: {
            String typeName = (String) value;
            descriptor = serCtx.getDescriptorByName(typeName);
            break;
         }
         case WrappedMessage.WRAPPED_DESCRIPTOR_TYPE_ID: {
            Integer typeId = (Integer) value;
            descriptor = serCtx.getDescriptorByTypeId(typeId);
            break;
         }
         case WrappedMessage.WRAPPED_MESSAGE:
            messageBytes = (byte[]) value;
            break;
         default:
            throw new IllegalStateException("Unexpected field : " + fieldNumber);
      }
   }

   @Override
   public void onStartNested(int fieldNumber, FieldDescriptor fieldDescriptor) {
      throw new IllegalStateException("Invalid WrappedMessage! Nested messages are not expected.");
   }

   @Override
   public void onEndNested(int fieldNumber, FieldDescriptor fieldDescriptor) {
      throw new IllegalStateException("Invalid WrappedMessage! Nested messages are not expected.");
   }

   @Override
   public void onEnd() {
      if (messageBytes != null) {
         // it's a message, not a primitive value; we must have a type now
         if (descriptor == null) {
            throw new IllegalStateException("Invalid WrappedMessage! Either type name or type id fields must be present but none was encountered.");
         }

         valueWrapper.setMessageDescriptor((Descriptor) descriptor);
      }
   }

   public byte[] getMessageBytes() {
      return messageBytes;
   }
}
