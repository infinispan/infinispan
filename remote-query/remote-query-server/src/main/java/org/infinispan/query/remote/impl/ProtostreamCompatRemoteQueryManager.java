package org.infinispan.query.remote.impl;

import java.io.IOException;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;

/**
 * Handle remote queries with deserialized object storage using the protostream marshaller.
 *
 * @since 9.2
 */
class ProtostreamCompatRemoteQueryManager extends AbstractCompatRemoteQueryManager {

   ProtostreamCompatRemoteQueryManager(ComponentRegistry cr) {
      super(cr);
   }

   @Override
   EntityNameResolver createEntityNamesResolver(ComponentRegistry cr) {
      return new ProtobufEntityNameResolver(ctx);
   }

   @Override
   CompatibilityReflectionMatcher createMatcher(EntityNameResolver entityNameResolver, SerializationContext ctx, SearchIntegrator searchFactory) {
      if (searchFactory == null) {
         return new CompatibilityReflectionMatcher(entityNameResolver, ctx);
      } else {
         return new CompatibilityReflectionMatcher(entityNameResolver, ctx, searchFactory);
      }
   }

   @Override
   public QueryRequest decodeQueryRequest(byte[] queryRequest) {
      try {
         return ProtobufUtil.fromByteArray(ctx, queryRequest, 0, queryRequest.length, QueryRequest.class);
      } catch (IOException e) {
         throw new CacheException();
      }
   }

   @Override
   public byte[] encodeQueryResponse(QueryResponse queryResponse) {
      try {
         return ProtobufUtil.toByteArray(ctx, queryResponse);
      } catch (IOException e) {
         throw new CacheException();
      }
   }

}
