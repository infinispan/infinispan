package org.infinispan.query.remote.impl;

import java.io.IOException;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.commons.CacheException;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.embedded.impl.HibernateSearchPropertyHelper;

/**
 * A sub-class of ReflectionMatcher that is able to lookup classes by their protobuf type name and can work when
 * compatibility mode is used.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class CompatibilityReflectionMatcher extends ReflectionMatcher {

   private final SerializationContext serializationContext;

   CompatibilityReflectionMatcher(SerializationContext serializationContext, SearchIntegrator searchFactory) {
      super(new HibernateSearchPropertyHelper(searchFactory, getEntityNameResolver(serializationContext)));
      this.serializationContext = serializationContext;
   }

   CompatibilityReflectionMatcher(SerializationContext serializationContext) {
      super(getEntityNameResolver(serializationContext));
      this.serializationContext = serializationContext;
   }

   private static EntityNameResolver getEntityNameResolver(SerializationContext serializationContext) {
      return entityName -> serializationContext.canMarshall(entityName) ?
            serializationContext.getMarshaller(entityName).getJavaClass() : null;
   }

   /**
    * Marshals the instance using Protobuf.
    *
    * @param instance never null
    * @return the converted/decorated instance
    */
   @Override
   protected Object convert(Object instance) {
      try {
         return ProtobufUtil.toWrappedByteArray(serializationContext, instance);
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }
}
