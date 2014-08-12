package org.infinispan.client.hotrod.impl.query;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.impl.BaseQueryFactory;
import org.infinispan.query.remote.client.MarshallerRegistration;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class RemoteQueryFactory extends BaseQueryFactory<Query> {

   private final RemoteCacheImpl cache;
   private final SerializationContext serializationContext;

   public RemoteQueryFactory(RemoteCacheImpl cache) {
      serializationContext = ProtoStreamMarshaller.getSerializationContext(cache.getRemoteCacheManager());

      this.cache = cache;

      try {
         MarshallerRegistration.registerMarshallers(serializationContext);
      } catch (Exception e) {
         //todo [anistor] need better exception handling
         throw new HotRodClientException("Failed to initialise serialization context", e);
      }
   }

   @Override
   public QueryBuilder<Query> from(Class entityType) {
      String typeName = serializationContext.getMarshaller(entityType).getTypeName();
      return new RemoteQueryBuilder(this, cache, serializationContext, typeName);
   }

   @Override
   public QueryBuilder<Query> from(String entityType) {
      return new RemoteQueryBuilder(this, cache, serializationContext, entityType);
   }
}
