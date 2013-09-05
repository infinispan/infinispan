package org.infinispan.query.remote.search;

import com.google.protobuf.Descriptors;
import org.hibernate.hql.lucene.internal.ast.HSearchTypeDescriptor;
import org.infinispan.query.remote.indexing.ProtobufValueWrapper;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
final class ProtobufValueWrapperTypeDescriptor implements HSearchTypeDescriptor {

   private final Descriptors.Descriptor messageDescriptor;

   public ProtobufValueWrapperTypeDescriptor(Descriptors.Descriptor messageDescriptor) {
      this.messageDescriptor = messageDescriptor;
   }

   public Descriptors.Descriptor getMessageDescriptor() {
      return messageDescriptor;
   }

   @Override
   public Class<?> getIndexedEntityType() {
      return ProtobufValueWrapper.class;
   }

   @Override
   public boolean isAnalyzed(String propertyName) {
      return false;
   }

   @Override
   public boolean isEmbedded(String propertyName) {
      Descriptors.FieldDescriptor field = messageDescriptor.findFieldByName(propertyName);
      return field != null && field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE;
   }

   @Override
   public boolean hasProperty(String propertyName) {
      return messageDescriptor.findFieldByName(propertyName) != null;
   }
}
