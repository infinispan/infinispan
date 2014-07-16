package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.impl.hql.FilterProcessingChain;
import org.infinispan.objectfilter.impl.hql.ProtobufEntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.ProtobufPropertyHelper;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.ProtobufMatcherEvalContext;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.JavaType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ProtobufMatcher extends BaseMatcher<Descriptor, Integer> {

   private final SerializationContext serializationContext;

   private final ProtobufEntityNamesResolver entityNamesResolver;

   private final ProtobufPropertyHelper propertyHelper;

   private final Descriptor wrappedMessageDescriptor;

   public ProtobufMatcher(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
      wrappedMessageDescriptor = serializationContext.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
      entityNamesResolver = new ProtobufEntityNamesResolver(serializationContext);
      propertyHelper = new ProtobufPropertyHelper(entityNamesResolver, serializationContext);
   }

   @Override
   protected MatcherEvalContext<Integer> startContext(Object instance, Set<String> knownTypes) {
      ProtobufMatcherEvalContext ctx = new ProtobufMatcherEvalContext(instance, wrappedMessageDescriptor, serializationContext);
      ctx.unwrapPayload();
      return ctx.getEntityTypeName() != null && knownTypes.contains(ctx.getEntityTypeName()) ? ctx : null;
   }

   @Override
   protected FilterProcessingChain<Descriptor> createFilterProcessingChain(Map<String, Object> namedParameters) {
      return FilterProcessingChain.build(entityNamesResolver, propertyHelper, namedParameters);
   }

   @Override
   protected FilterRegistry<Integer> createFilterRegistryForType(Descriptor messageDescriptor) {
      return new FilterRegistry<Integer>(new MetadataAdapterImpl(messageDescriptor));
   }

   private static class MetadataAdapterImpl implements MetadataAdapter<FieldDescriptor, Integer> {

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
      public List<Integer> translatePropertyPath(List<String> path) {
         List<Integer> propPath = new ArrayList<Integer>(path.size());
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
      public boolean isRepeatedProperty(List<String> propertyPath) {
         Descriptor md = messageDescriptor;
         for (String prop : propertyPath) {
            FieldDescriptor fd = md.findFieldByName(prop);
            if (fd.isRepeated()) {
               return true;
            }
            if (fd.getJavaType() == JavaType.MESSAGE) {
               md = fd.getMessageType();
            } else {
               md = null; // iteration is expected to stop here
            }
         }
         return false;
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
