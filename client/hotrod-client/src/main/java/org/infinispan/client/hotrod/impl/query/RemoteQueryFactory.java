package org.infinispan.client.hotrod.impl.query;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.impl.BaseQueryFactory;
import org.infinispan.query.remote.client.impl.MarshallerRegistration;
import org.infinispan.query.remote.client.impl.QueryRequest;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class RemoteQueryFactory extends BaseQueryFactory {

   private final InternalRemoteCache<?, ?> cache;
   private final SerializationContext serializationContext;

   public RemoteQueryFactory(InternalRemoteCache<?, ?> cache) {
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
      return new RemoteQuery(this, cache, serializationContext, queryString, null);
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
