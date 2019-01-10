package org.infinispan.client.hotrod.impl.query;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.impl.BaseQueryFactory;
import org.infinispan.query.remote.client.MarshallerRegistration;
import org.infinispan.query.remote.client.QueryRequest;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class RemoteQueryFactory extends BaseQueryFactory {

   private final RemoteCacheImpl<?, ?> cache;
   private final SerializationContext serializationContext;

   public RemoteQueryFactory(RemoteCacheImpl<?, ?> cache) {
      this.cache = cache;
      Marshaller marshaller = cache.getRemoteCacheManager().getMarshaller();
      // we may or may not use Protobuf
      if (marshaller instanceof ProtoStreamMarshaller) {
         serializationContext = ((ProtoStreamMarshaller) marshaller).getSerializationContext();
         try {
            if (!serializationContext.canMarshall(QueryRequest.class)) {
               MarshallerRegistration.init(serializationContext);
            }
         } catch (Exception e) {
            throw new HotRodClientException("Failed to initialise the Protobuf serialization context", e);
         }
      } else {
         serializationContext = null;
      }
   }

   @Override
   public Query create(String queryString) {
      return new RemoteQuery(this, cache, serializationContext, queryString, IndexedQueryMode.FETCH);
   }

   @Override
   public Query create(String queryString, IndexedQueryMode queryMode) {
      return new RemoteQuery(this, cache, serializationContext, queryString, queryMode);
   }

   @Override
   public QueryBuilder from(Class<?> entityType) {
      String typeName = serializationContext != null ?
            serializationContext.getMarshaller(entityType).getTypeName() : entityType.getName();
      return new RemoteQueryBuilder(this, cache, serializationContext, typeName);
   }

   @Override
   public QueryBuilder from(String entityType) {
      if (serializationContext != null) {
         // just check that the type name is valid
         serializationContext.getMarshaller(entityType);
      }
      return new RemoteQueryBuilder(this, cache, serializationContext, entityType);
   }
}
