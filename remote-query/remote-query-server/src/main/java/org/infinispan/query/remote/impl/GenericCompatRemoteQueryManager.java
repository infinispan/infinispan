package org.infinispan.query.remote.impl;

import java.util.Set;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;

/**
 * Handle remote queries with deserialized object storage using the configured compat mode marshaller.
 *
 * @since 9.2
 */
class GenericCompatRemoteQueryManager extends AbstractCompatRemoteQueryManager {

   GenericCompatRemoteQueryManager(ComponentRegistry cr) {
      super(cr);
   }

   @Override
   EntityNameResolver createEntityNamesResolver(ComponentRegistry cr) {
      ClassLoader classLoader = cr.getGlobalComponentRegistry().getComponent(ClassLoader.class);

      EntityNameResolver entityNameResolver;
      ReflectionEntityNamesResolver reflectionEntityNamesResolver = new ReflectionEntityNamesResolver(classLoader);
      if (isIndexed) {
         Set<Class<?>> knownClasses = cr.getComponent(QueryInterceptor.class).getKnownClasses();
         // If indexing is enabled, then use the known set of classes for lookup and the global classloader as a fallback.
         entityNameResolver = name -> knownClasses.stream()
               .filter(c -> c.getName().equals(name))
               .findFirst()
               .orElse(reflectionEntityNamesResolver.resolve(name));
      } else {
         entityNameResolver = reflectionEntityNamesResolver;
      }
      return entityNameResolver;
   }

   @Override
   CompatibilityReflectionMatcher createMatcher(EntityNameResolver entityNameResolver, SerializationContext ctx, SearchIntegrator searchIntegrator) {
      if (searchIntegrator == null) {
         return new CompatibilityReflectionMatcher(entityNameResolver, null);
      } else {
         return new CompatibilityReflectionMatcher(entityNameResolver, null, searchIntegrator);
      }
   }

   @Override
   public QueryRequest decodeQueryRequest(byte[] queryRequest) {
      return (QueryRequest) getValueEncoder().toStorage(queryRequest);
   }

   @Override
   public byte[] encodeQueryResponse(QueryResponse queryResponse) {
      Object o = this.getValueEncoder().fromStorage(queryResponse);
      return (byte[]) o;
   }

}
