package org.infinispan.objectfilter.impl;

import com.google.protobuf.Descriptors;
import org.infinispan.objectfilter.impl.hql.FilterProcessingChain;
import org.infinispan.objectfilter.impl.hql.ProtobufPropertyHelper;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.ProtobufMatcherEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.be.BETreeMaker;
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
   protected FilterProcessingChain<?> createFilterProcessingChain(Map<String, Object> namedParameters) {
      return FilterProcessingChain.build(new ProtobufPropertyHelper(serializationContext), namedParameters);
   }

   @Override
   protected FilterRegistry<Integer> createFilterRegistryForType(final Descriptors.Descriptor messageDescriptor) {
      return new FilterRegistry<Integer>(new BETreeMaker.AttributePathTranslator<Integer>() {
         @Override
         public List<Integer> translatePath(List<String> path) {
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
         public boolean isRepeated(List<String> path) {
            Descriptors.Descriptor md = messageDescriptor;
            for (String prop : path) {
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
      }, messageDescriptor.getFullName());
   }
}
