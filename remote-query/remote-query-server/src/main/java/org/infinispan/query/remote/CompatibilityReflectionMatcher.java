package org.infinispan.query.remote;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.SerializationContext;

/**
 * A sub-class of ReflectionMatcher that is able to lookup classes by their protobuf type name and can work when
 * compatibility mode is used.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class CompatibilityReflectionMatcher extends ReflectionMatcher {

   public CompatibilityReflectionMatcher(final SerializationContext serializationContext) {
      super(new EntityNamesResolver() {
         @Override
         public Class<?> getClassFromName(String entityName) {
            try {
               MessageMarshaller messageMarshaller = (MessageMarshaller) serializationContext.getMarshaller(entityName);
               return messageMarshaller.getJavaClass();
            } catch (Exception e) {
               return null;
            }
         }
      });
   }
}
