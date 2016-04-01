package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.impl.hql.JPQLParser;
import org.infinispan.objectfilter.impl.hql.ProtobufEntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.ProtobufPropertyHelper;
import org.infinispan.objectfilter.impl.predicateindex.ProtobufMatcherEvalContext;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.JavaType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ProtobufMatcher extends BaseMatcher<Descriptor, FieldDescriptor, Integer> {

   private final SerializationContext serializationContext;

   private final ProtobufPropertyHelper propertyHelper;

   private final Descriptor wrappedMessageDescriptor;

   private final JPQLParser<Descriptor> parser;

   public ProtobufMatcher(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
      wrappedMessageDescriptor = serializationContext.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
      ProtobufEntityNamesResolver entityNamesResolver = new ProtobufEntityNamesResolver(serializationContext);
      propertyHelper = new ProtobufPropertyHelper(entityNamesResolver, serializationContext);
      parser = new JPQLParser<>(entityNamesResolver, propertyHelper);
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
      return filtersByTypeName.get(entityType.getFullName());
   }

   @Override
   public JPQLParser<Descriptor> getParser() {
      return parser;
   }

   @Override
   public ProtobufPropertyHelper getPropertyHelper() {
      return propertyHelper;
   }

   @Override
   protected MetadataAdapter<Descriptor, FieldDescriptor, Integer> createMetadataAdapter(Descriptor messageDescriptor) {
      return new MetadataAdapterImpl(messageDescriptor);
   }

   private static class MetadataAdapterImpl implements MetadataAdapter<Descriptor, FieldDescriptor, Integer> {

      private final Descriptor messageDescriptor;

      MetadataAdapterImpl(Descriptor messageDescriptor) {
         this.messageDescriptor = messageDescriptor;
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
      public List<Integer> translatePropertyPath(String[] path) {
         List<Integer> propPath = new ArrayList<>(path.length);
         Descriptor md = messageDescriptor;
         for (String prop : path) {
            FieldDescriptor fd = md.findFieldByName(prop);
            propPath.add(fd.getNumber());
            if (fd.getJavaType() == JavaType.MESSAGE) {
               md = fd.getMessageType();
            } else {
               md = null; // iteration is expected to stop here
            }
         }
         return propPath;
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
