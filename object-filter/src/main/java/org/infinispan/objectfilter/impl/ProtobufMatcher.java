package org.infinispan.objectfilter.impl;

import java.util.List;

import org.infinispan.objectfilter.impl.predicateindex.ProtobufMatcherEvalContext;
import org.infinispan.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.objectfilter.impl.syntax.parser.ObjectPropertyHelper;
import org.infinispan.objectfilter.impl.syntax.parser.ProtobufPropertyHelper;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ProtobufMatcher extends BaseMatcher<Descriptor, FieldDescriptor, Integer> {

   private final SerializationContext serializationContext;

   private final Descriptor wrappedMessageDescriptor;

   public ProtobufMatcher(SerializationContext serializationContext, IndexedFieldProvider<Descriptor> indexedFieldProvider) {
      super(new ProtobufPropertyHelper(serializationContext, indexedFieldProvider));
      this.serializationContext = serializationContext;
      this.wrappedMessageDescriptor = serializationContext.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
   }

   @Override
   protected ProtobufMatcherEvalContext startMultiTypeContext(Object userContext, Object eventType, Object instance) {
      ProtobufMatcherEvalContext context = new ProtobufMatcherEvalContext(userContext, eventType, instance, wrappedMessageDescriptor, serializationContext);
      if (context.getEntityType() != null) {
         FilterRegistry<Descriptor, FieldDescriptor, Integer> filterRegistry = getFilterRegistryForType(context.getEntityType());
         if (filterRegistry != null) {
            context.initMultiFilterContext(filterRegistry);
            return context;
         }
      }
      return null;
   }

   @Override
   protected ProtobufMatcherEvalContext startSingleTypeContext(Object userContext, Object eventType, Object instance, MetadataAdapter<Descriptor, FieldDescriptor, Integer> metadataAdapter) {
      ProtobufMatcherEvalContext ctx = new ProtobufMatcherEvalContext(userContext, eventType, instance, wrappedMessageDescriptor, serializationContext);
      return ctx.getEntityType() != null && ctx.getEntityType().getFullName().equals(metadataAdapter.getTypeName()) ? ctx : null;
   }

   @Override
   protected FilterRegistry<Descriptor, FieldDescriptor, Integer> getFilterRegistryForType(Descriptor entityType) {
      return filtersByType.get(entityType);
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
         switch (attributeMetadata.getJavaType()) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case STRING:
            case BYTE_STRING:
            case ENUM:
               return true;
         }
         return false;
      }
   }
}
