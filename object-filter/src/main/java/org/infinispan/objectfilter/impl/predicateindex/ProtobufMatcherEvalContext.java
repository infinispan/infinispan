package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.protostream.MessageContext;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.TagHandler;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.JavaType;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.protostream.impl.WrappedMessageMarshaller;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ProtobufMatcherEvalContext extends MatcherEvalContext<Descriptor, FieldDescriptor, Integer> implements TagHandler {

   private static final Object DUMMY_VALUE = new Object();

   private boolean payloadStarted = false;
   private int skipping = 0;

   private byte[] payload;
   private String entityTypeName;
   private Descriptor payloadMessageDescriptor;
   private MessageContext messageContext;

   private final SerializationContext serializationContext;
   private final Descriptor wrappedMessageDescriptor;

   public ProtobufMatcherEvalContext(Object instance, Descriptor wrappedMessageDescriptor, SerializationContext serializationContext) {
      super(instance);
      this.wrappedMessageDescriptor = wrappedMessageDescriptor;
      this.serializationContext = serializationContext;
   }

   @Override
   public Descriptor getEntityType() {
      return payloadMessageDescriptor;
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

   //todo [anistor] missing tags need to be fired with default value defined in proto schema or null if they admit null; missing messages need to be fired with null at end of the nesting level. BTW, seems like this is better to be included in Protostream as a feature
   @Override
   public void onTag(int fieldNumber, String fieldName, Type type, JavaType javaType, Object tagValue) {
      if (payloadStarted) {
         if (skipping == 0) {
            AttributeNode<FieldDescriptor, Integer> attrNode = currentNode.getChild(fieldNumber);
            if (attrNode != null) { // process only 'interesting' tags
               messageContext.markField(fieldNumber);
               attrNode.processValue(tagValue, this);
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
   public void onStartNested(int fieldNumber, String fieldName, Descriptor messageDescriptor) {
      if (payloadStarted) {
         if (skipping == 0) {
            AttributeNode<FieldDescriptor, Integer> attrNode = currentNode.getChild(fieldNumber);
            if (attrNode != null) { // ignore 'uninteresting' tags
               messageContext.markField(fieldNumber);
               pushContext(fieldName, messageDescriptor);
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
   public void onEndNested(int fieldNumber, String fieldName, Descriptor messageDescriptor) {
      if (payloadStarted) {
         if (skipping == 0) {
            popContext();
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
      if (payloadStarted) {
         processMissingFields();
      } else {
         payloadStarted = true;

         if (payload != null) {
            if (entityTypeName == null) {
               throw new IllegalStateException("Descriptor name is missing");
            }

            payloadMessageDescriptor = serializationContext.getMessageDescriptor(entityTypeName);
            messageContext = new MessageContext<MessageContext>(null, null, payloadMessageDescriptor);
         }
      }
   }

   @Override
   protected void processAttributes(AttributeNode<FieldDescriptor, Integer> node, Object instance) {
      try {
         ProtobufParser.INSTANCE.parse(this, payloadMessageDescriptor, payload);
      } catch (IOException e) {
         throw new RuntimeException(e);  // TODO [anistor] proper exception handling needed
      }
   }

   private void pushContext(String fieldName, Descriptor messageDescriptor) {
      messageContext = new MessageContext<MessageContext>(messageContext, fieldName, messageDescriptor);
   }

   private void popContext() {
      processMissingFields();
      messageContext = messageContext.getParentContext();
   }

   private void processMissingFields() {
      for (FieldDescriptor fd : messageContext.getMessageDescriptor().getFields()) {
         AttributeNode<FieldDescriptor, Integer> attributeNode = currentNode.getChild(fd.getNumber());
         boolean fieldSeen = messageContext.isFieldMarked(fd.getNumber());
         if (attributeNode != null && (fd.isRepeated() || !fieldSeen)) {
            if (fd.isRepeated()) {
               // Repeated fields can't have default values but we need to at least take care of IS [NOT] NULL predicates
               if (fieldSeen) {
                  // Here we use a dummy value since it would not matter anyway for IS [NOT] NULL
                  attributeNode.processValue(DUMMY_VALUE, this);
               } else {
                  processNullAttribute(attributeNode);
               }
            } else {
               if (fd.getJavaType() == JavaType.MESSAGE) {
                  processNullAttribute(attributeNode);
               } else {
                  Object defaultValue = fd.hasDefaultValue() ? fd.getDefaultValue() : null;
                  attributeNode.processValue(defaultValue, this);
               }
            }
         }
      }
   }

   private void processNullAttribute(AttributeNode<FieldDescriptor, Integer> attributeNode) {
      attributeNode.processValue(null, this);
      for (AttributeNode<FieldDescriptor, Integer> childAttribute : attributeNode.getChildren()) {
         processNullAttribute(childAttribute);
      }
   }
}
