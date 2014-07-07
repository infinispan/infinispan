package org.infinispan.objectfilter.impl;

import com.google.protobuf.Descriptors;
import org.infinispan.objectfilter.impl.hql.FilterProcessingChain;
import org.infinispan.objectfilter.impl.hql.ProtobufPropertyHelper;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.ProtobufMatcherEvalContext;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ProtobufMatcher extends BaseMatcher<Descriptors.Descriptor, Integer> {

   private final SerializationContext serializationContext;

   private final Descriptors.Descriptor wrappedMessageDescriptor;

   public ProtobufMatcher(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
      wrappedMessageDescriptor = serializationContext.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
   }

   @Override
   protected MatcherEvalContext<Integer> startContext(Object instance, Set<String> knownTypes) {
      ProtobufMatcherEvalContext ctx = new ProtobufMatcherEvalContext(instance, wrappedMessageDescriptor, serializationContext);
      ctx.unwrapPayload();
      return ctx.getEntityTypeName() != null && knownTypes.contains(ctx.getEntityTypeName()) ? ctx : null;
   }

   @Override
   protected FilterProcessingChain<Descriptors.Descriptor> createFilterProcessingChain(Map<String, Object> namedParameters) {
      return FilterProcessingChain.build(new ProtobufPropertyHelper(serializationContext), namedParameters);
   }

   @Override
   protected FilterRegistry<Integer> createFilterRegistryForType(Descriptors.Descriptor messageDescriptor) {
      return new FilterRegistry<Integer>(new MetadataAdapterImpl(messageDescriptor));
   }

   private static class MetadataAdapterImpl implements MetadataAdapter<Descriptors.FieldDescriptor, Integer> {

      private final Descriptors.Descriptor messageDescriptor;

      MetadataAdapterImpl(Descriptors.Descriptor messageDescriptor) {
         this.messageDescriptor = messageDescriptor;
      }

      @Override
      public String getTypeName() {
         return messageDescriptor.getFullName();
      }

      @Override
      public Descriptors.Descriptor getTypeMetadata() {
         return messageDescriptor;
      }

      @Override
      public List<Integer> translatePropertyPath(List<String> path) {
         List<Integer> propPath = new ArrayList<Integer>(path.size());
         Descriptors.Descriptor md = messageDescriptor;
         for (String prop : path) {
            Descriptors.FieldDescriptor fd = md.findFieldByName(prop);
            propPath.add(fd.getNumber());
            if (fd.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
               md = fd.getMessageType();
            } else {
               md = null; // iteration is expected to stop here
            }
         }
         return propPath;
      }

      @Override
      public boolean isRepeatedProperty(List<String> propertyPath) {
         Descriptors.Descriptor md = messageDescriptor;
         for (String prop : propertyPath) {
            Descriptors.FieldDescriptor fd = md.findFieldByName(prop);
            if (fd.isRepeated()) {
               return true;
            }
            if (fd.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
               md = fd.getMessageType();
            } else {
               md = null; // iteration is expected to stop here
            }
         }
         return false;
      }

      @Override
      public Descriptors.FieldDescriptor makeChildAttributeMetadata(Descriptors.FieldDescriptor parentAttributeMetadata, Integer attribute) {
         return parentAttributeMetadata == null ?
               messageDescriptor.findFieldByNumber(attribute) : parentAttributeMetadata.getMessageType().findFieldByNumber(attribute);
      }

      @Override
      public boolean isComparableProperty(Descriptors.FieldDescriptor attributeMetadata) {
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
