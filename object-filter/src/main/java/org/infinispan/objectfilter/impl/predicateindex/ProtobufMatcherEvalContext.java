package org.infinispan.objectfilter.impl.predicateindex;

import com.google.protobuf.Descriptors;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.TagHandler;
import org.infinispan.protostream.impl.WrappedMessageMarshaller;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ProtobufMatcherEvalContext extends MatcherEvalContext<Integer> implements TagHandler {

   private boolean payloadStarted = false;
   private int skipping = 0;

   private byte[] payload;
   private Descriptors.Descriptor payloadMessageDescriptor;

   private final SerializationContext serializationContext;
   private final Descriptors.Descriptor wrappedMessageDescriptor;

   public ProtobufMatcherEvalContext(Object instance, Descriptors.Descriptor wrappedMessageDescriptor, SerializationContext serializationContext) {
      super(instance);
      this.wrappedMessageDescriptor = wrappedMessageDescriptor;
      this.serializationContext = serializationContext;
   }

   public void unwrapPayload() {
      try {
         ProtobufParser.INSTANCE.parse(this, wrappedMessageDescriptor, (byte[]) getInstance());
      } catch (IOException e) {
         throw new RuntimeException(e);  // TODO [anistor] proper exception handling needed
      }
   }

   @Override
   public void onStart() {
   }

   //todo [anistor] missing tags need to be fired with default value defined in proto schema; missing messages need to be fired with null at end of the nesting level. BTW, seems like this is better to be included in Protostream as a feature
   @Override
   public void onTag(int fieldNumber, String fieldName, Descriptors.FieldDescriptor.Type type, Descriptors.FieldDescriptor.JavaType javaType, Object tagValue) {
      if (payloadStarted) {
         if (skipping == 0) {
            AttributeNode<Integer> attrNode = currentNode.getChild(fieldNumber);
            if (attrNode != null) { // process only 'interesting' tags
               attrNode.dispatchValueToPredicates(tagValue, this);
            }
         }
      } else {
         switch (fieldNumber) {
            case WrappedMessageMarshaller.WRAPPED_DESCRIPTOR_FULL_NAME:
               entityTypeName = (String) tagValue;
               break;

            case WrappedMessageMarshaller.WRAPPED_MESSAGE_BYTES:
               payload = (byte[]) tagValue;
               break;

            default:
               throw new IllegalStateException("Unexpected field : " + fieldNumber);
         }
      }
   }

   @Override
   public void onStartNested(int fieldNumber, String fieldName, Descriptors.Descriptor messageDescriptor) {
      if (payloadStarted) {
         if (skipping == 0) {
            AttributeNode<Integer> attrNode = currentNode.getChild(fieldNumber);
            if (attrNode != null) { // ignore 'uninteresting' tags
               currentNode = attrNode;
               return;
            }
         }

         // found an uninteresting nesting level, start skipping from here on until this level ends
         skipping++;
      } else {
         throw new IllegalStateException("No nested message is expected");
      }
   }

   @Override
   public void onEndNested(int fieldNumber, String fieldName, Descriptors.Descriptor messageDescriptor) {
      if (payloadStarted) {
         if (skipping == 0) {
            currentNode = currentNode.getParent();
         } else {
            skipping--;
         }
      } else {
         throw new IllegalStateException("No nested message is expected");
      }
   }

   @Override
   public void onEnd() {
      if (!payloadStarted) {
         payloadStarted = true;

         if (payload != null) {
            if (entityTypeName == null) {
               throw new IllegalStateException("Descriptor name is missing");
            }

            payloadMessageDescriptor = serializationContext.getMessageDescriptor(entityTypeName);
         }
      }
   }

   @Override
   protected void processAttributes(AttributeNode<Integer> node, Object instance) {
      try {
         ProtobufParser.INSTANCE.parse(this, payloadMessageDescriptor, payload);
      } catch (IOException e) {
         throw new RuntimeException(e);  // TODO [anistor] proper exception handling needed
      }
   }
}
