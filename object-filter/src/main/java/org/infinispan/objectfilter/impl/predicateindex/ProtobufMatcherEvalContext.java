package org.infinispan.objectfilter.impl.predicateindex;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.syntax.parser.ProtobufPropertyHelper;
import org.infinispan.protostream.MessageContext;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.TagHandler;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.GenericDescriptor;
import org.infinispan.protostream.descriptors.JavaType;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ProtobufMatcherEvalContext extends MatcherEvalContext<Descriptor, FieldDescriptor, Integer> implements TagHandler {

   private static final Log log = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, ProtobufMatcherEvalContext.class.getName());

   private boolean payloadStarted = false;
   private int skipping = 0;

   private byte[] payload;
   private String entityTypeName;
   private Descriptor payloadMessageDescriptor;
   private MessageContext messageContext;

   private final SerializationContext serializationContext;

   public ProtobufMatcherEvalContext(Object userContext, Object eventType, Object key, Object instance, Object metadata,
                                     Descriptor wrappedMessageDescriptor, SerializationContext serializationContext) {
      super(userContext, eventType, key, instance, metadata);
      this.serializationContext = serializationContext;
      try {
         ProtobufParser.INSTANCE.parse(this, wrappedMessageDescriptor, (byte[]) getInstance());
      } catch (IOException e) {
         throw log.errorParsingProtobuf(e);
      }
   }

   @Override
   public Descriptor getEntityType() {
      return payloadMessageDescriptor;
   }

   @Override
   public void onStart(GenericDescriptor descriptor) {
   }

   //todo [anistor] missing tags need to be fired with default value defined in proto schema or null if they admit null; missing messages need to be fired with null at end of the nesting level. BTW, seems like this is better to be included in Protostream as a feature
   @Override
   public void onTag(int fieldNumber, FieldDescriptor fieldDescriptor, Object tagValue) {
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
            case WrappedMessage.WRAPPED_DESCRIPTOR_FULL_NAME:
               entityTypeName = (String) tagValue;
               break;

            case WrappedMessage.WRAPPED_DESCRIPTOR_TYPE_ID:
               entityTypeName = serializationContext.getDescriptorByTypeId((Integer) tagValue).getFullName();
               break;

            case WrappedMessage.WRAPPED_MESSAGE:
               payload = (byte[]) tagValue;
               break;

            case WrappedMessage.WRAPPED_DOUBLE:
            case WrappedMessage.WRAPPED_FLOAT:
            case WrappedMessage.WRAPPED_INT64:
            case WrappedMessage.WRAPPED_UINT64:
            case WrappedMessage.WRAPPED_INT32:
            case WrappedMessage.WRAPPED_FIXED64:
            case WrappedMessage.WRAPPED_FIXED32:
            case WrappedMessage.WRAPPED_BOOL:
            case WrappedMessage.WRAPPED_STRING:
            case WrappedMessage.WRAPPED_BYTES:
            case WrappedMessage.WRAPPED_UINT32:
            case WrappedMessage.WRAPPED_SFIXED32:
            case WrappedMessage.WRAPPED_SFIXED64:
            case WrappedMessage.WRAPPED_SINT32:
            case WrappedMessage.WRAPPED_SINT64:
            case WrappedMessage.WRAPPED_ENUM:
               break;
            // this is a primitive value, which we ignore for now due to lack of support for querying primitives

            default:
               throw new IllegalStateException("Unexpected field : " + fieldNumber);
         }
      }
   }

   @Override
   public void onStartNested(int fieldNumber, FieldDescriptor fieldDescriptor) {
      if (payloadStarted) {
         if (skipping == 0) {
            AttributeNode<FieldDescriptor, Integer> attrNode = currentNode.getChild(fieldNumber);
            if (attrNode != null) { // ignore 'uninteresting' tags
               messageContext.markField(fieldNumber);
               pushContext(fieldDescriptor, fieldDescriptor.getMessageType());
               currentNode = attrNode;
               return;
            }
         }

         // found an uninteresting nesting level, start skipping from here on until this level ends
         skipping++;
      } else {
         throw new IllegalStateException("No nested message is supported");
      }
   }

   @Override
   public void onEndNested(int fieldNumber, FieldDescriptor fieldDescriptor) {
      if (payloadStarted) {
         if (skipping == 0) {
            popContext();
            currentNode = currentNode.getParent();
         } else {
            skipping--;
         }
      } else {
         throw new IllegalStateException("No nested message is supported");
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
            messageContext = new MessageContext<>(null, null, payloadMessageDescriptor);
         }
      }
   }

   @Override
   protected void processAttributes(AttributeNode<FieldDescriptor, Integer> node, Object instance) {
      try {
         ProtobufParser.INSTANCE.parse(this, payloadMessageDescriptor, payload);
         for (AttributeNode<FieldDescriptor, Integer> childAttribute : node.getChildren()) {
            if (childAttribute.getAttribute() >= ProtobufPropertyHelper.MIN_METADATA_FIELD_ATTRIBUTE_ID) {
               Object attributeValue = node.cacheMetadataProjection(key, instance, metadata, childAttribute.getAttribute());
               childAttribute.processValue(attributeValue, this);
            }
         }
      } catch (IOException e) {
         throw new RuntimeException(e);  // TODO [anistor] proper exception handling needed
      }
   }

   private void pushContext(FieldDescriptor fieldDescriptor, Descriptor messageDescriptor) {
      messageContext = new MessageContext<>(messageContext, fieldDescriptor, messageDescriptor);
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
                  attributeNode.processValue(AttributeNode.DUMMY_VALUE, this);
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
