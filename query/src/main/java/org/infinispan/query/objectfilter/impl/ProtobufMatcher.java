package org.infinispan.query.objectfilter.impl;

import java.util.List;

import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.objectfilter.impl.predicateindex.ProtobufMatcherEvalContext;
import org.infinispan.query.objectfilter.impl.syntax.parser.ObjectPropertyHelper;
import org.infinispan.query.objectfilter.impl.syntax.parser.ProtobufPropertyHelper;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ProtobufMatcher extends BaseMatcher<Descriptor, FieldDescriptor, Integer> {

   private final SerializationContext serializationContext;

   private final Descriptor wrappedMessageDescriptor;

   public ProtobufMatcher(SerializationContext serializationContext, ProtobufPropertyHelper propertyHelper) {
      super(propertyHelper != null ? propertyHelper : new ProtobufPropertyHelper(serializationContext, null));
      this.serializationContext = serializationContext;
      this.wrappedMessageDescriptor = serializationContext.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
   }

   @Override
   protected ProtobufMatcherEvalContext startMultiTypeContext(boolean isDeltaFilter, Object userContext, Object eventType, Object instance) {
      ProtobufMatcherEvalContext context = new ProtobufMatcherEvalContext(userContext, eventType, null, instance, null,
            wrappedMessageDescriptor, serializationContext);
      if (context.getEntityType() != null) {
         FilterRegistry<Descriptor, FieldDescriptor, Integer> filterRegistry = getFilterRegistryForType(isDeltaFilter, context.getEntityType());
         if (filterRegistry != null) {
            context.initMultiFilterContext(filterRegistry);
            return context;
         }
      }
      return null;
   }

   @Override
   protected ProtobufMatcherEvalContext startSingleTypeContext(Object userContext, Object eventType, Object key,
                                                               Object instance, Object metadata,
                                                               MetadataAdapter<Descriptor, FieldDescriptor, Integer> metadataAdapter) {
      ProtobufMatcherEvalContext ctx = new ProtobufMatcherEvalContext(userContext, eventType, key, instance, metadata,
            wrappedMessageDescriptor, serializationContext);
      return ctx.getEntityType() != null && ctx.getEntityType().getFullName().equals(metadataAdapter.getTypeName()) ? ctx : null;
   }

   @Override
   protected MetadataAdapter<Descriptor, FieldDescriptor, Integer> createMetadataAdapter(Descriptor messageDescriptor) {
      return new MetadataAdapterImpl(messageDescriptor, propertyHelper);
   }

   private static class MetadataAdapterImpl implements MetadataAdapter<Descriptor, FieldDescriptor, Integer> {

      private final Descriptor messageDescriptor;
      private final ObjectPropertyHelper<Descriptor> propertyHelper;

      MetadataAdapterImpl(Descriptor messageDescriptor, ObjectPropertyHelper<Descriptor> propertyHelper) {
         this.messageDescriptor = messageDescriptor;
         this.propertyHelper = propertyHelper;
      }

      @Override
      public String getTypeName() {
         return messageDescriptor.getFullName();
      }

      @Override
      public Descriptor getTypeMetadata() {
         return messageDescriptor;
      }

      @Override
      public List<Integer> mapPropertyNamePathToFieldIdPath(String[] path) {
         return (List<Integer>) propertyHelper.mapPropertyNamePathToFieldIdPath(messageDescriptor, path);
      }

      @Override
      public FieldDescriptor makeChildAttributeMetadata(FieldDescriptor parentAttributeMetadata, Integer attribute) {
         return parentAttributeMetadata == null ?
               messageDescriptor.findFieldByNumber(attribute) : parentAttributeMetadata.getMessageType().findFieldByNumber(attribute);
      }

      @Override
      public boolean isComparableProperty(FieldDescriptor attributeMetadata) {
         return switch (attributeMetadata.getJavaType()) {
            case INT, LONG, FLOAT, DOUBLE, BOOLEAN, STRING, BYTE_STRING, ENUM -> true;
            default -> false;
         };
      }
   }
}
