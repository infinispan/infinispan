package org.infinispan.client.hotrod.impl.query;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.event.impl.ContinuousQueryImpl;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.api.query.ContinuousQuery;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.impl.MarshallerRegistration;
import org.infinispan.query.remote.client.impl.QueryRequest;

/**
 * @author anistor@redhat.com
 * @since 6.0
 * @deprecated Use {@link org.infinispan.commons.api.BasicCache#query(String)} and {@link BasicCache#continuousQuery()} instead.
 */
@Deprecated(since = "15.0", forRemoval = true)
public final class RemoteQueryFactory implements QueryFactory {

   private final InternalRemoteCache<?, ?> cache;
   private final SerializationContext serializationContext;

   public RemoteQueryFactory(InternalRemoteCache<?, ?> cache) {
      this.cache = cache;
      ProtoStreamMarshaller protoStreamMarshaller = (ProtoStreamMarshaller) cache.getRemoteCacheContainer().getMarshallerRegistry().getMarshaller(ProtoStreamMarshaller.class);
      // protostream might be absent
      if (protoStreamMarshaller != null) {
         serializationContext = protoStreamMarshaller.getSerializationContext();
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
   public <T> Query<T> create(String queryString) {
      return new RemoteQuery<>(cache, serializationContext, queryString);
   }

   public <K, V> ContinuousQuery<K, V> continuousQuery(RemoteCache<K, V> cache) {
      return new ContinuousQueryImpl<>(cache);
   }
}
