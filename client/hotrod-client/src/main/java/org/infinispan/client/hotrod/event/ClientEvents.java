package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryExpired;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.filter.Filters;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.remote.client.ContinuousQueryResult;

import java.io.IOException;

import static org.infinispan.client.hotrod.filter.Filters.*;

public class ClientEvents {

   private static final Log log = LogFactory.getLog(ClientEvents.class, Log.class);

   /**
    * The name of the factory used for query DSL based filters and converters. This factory is provided internally by
    * the server.
    * @deprecated Use constants from {@link Filters}
    */
   @Deprecated
   public static final String QUERY_DSL_FILTER_FACTORY_NAME = Filters.QUERY_DSL_FILTER_FACTORY_NAME;

   @Deprecated
   public static final String CONTINUOUS_QUERY_FILTER_FACTORY_NAME = Filters.CONTINUOUS_QUERY_FILTER_FACTORY_NAME;

   private static final ClientCacheFailoverEvent FAILOVER_EVENT_SINGLETON = new ClientCacheFailoverEvent() {
      @Override
      public ClientEvent.Type getType() {
         return ClientEvent.Type.CLIENT_CACHE_FAILOVER;
      }
   };

   private ClientEvents() {
      // Static helper class, cannot be constructed
   }

   public static ClientCacheFailoverEvent mkCachefailoverEvent() {
      return FAILOVER_EVENT_SINGLETON;
   }

   /**
    * Register a client listener that uses a query DSL based filter. The listener is expected to be annotated such that
    * {@link org.infinispan.client.hotrod.annotation.ClientListener#useRawData} = true and {@link
    * org.infinispan.client.hotrod.annotation.ClientListener#filterFactoryName} and {@link
    * org.infinispan.client.hotrod.annotation.ClientListener#converterFactoryName} are equal to {@link
    * Filters#QUERY_DSL_FILTER_FACTORY_NAME}
    *
    * @param remoteCache the remote cache to attach the listener
    * @param listener    the listener instance
    * @param query       the query to be used for filtering and conversion (if projections are used)
    */
   public static void addClientQueryListener(RemoteCache<?, ?> remoteCache, Object listener, Query query) {
      ClientListener l = ReflectionUtil.getAnnotation(listener.getClass(), ClientListener.class);
      if (l == null) {
         throw log.missingClientListenerAnnotation(listener.getClass().getName());
      }
      if (!l.useRawData()) {
         throw log.clientListenerMustUseRawData(listener.getClass().getName());
      }
      if (!l.filterFactoryName().equals(Filters.QUERY_DSL_FILTER_FACTORY_NAME)) {
         throw log.clientListenerMustUseDesignatedFilterConverterFactory(Filters.QUERY_DSL_FILTER_FACTORY_NAME);
      }
      if (!l.converterFactoryName().equals(Filters.QUERY_DSL_FILTER_FACTORY_NAME)) {
         throw log.clientListenerMustUseDesignatedFilterConverterFactory(Filters.QUERY_DSL_FILTER_FACTORY_NAME);
      }
      Object[] factoryParams = makeFactoryParams(query);
      remoteCache.addClientListener(listener, factoryParams, null);
   }

   /**
    * Registers a continuous query listener that uses a query DSL based filter. The listener will receive notifications
    * when a cache entry joins or leaves the matching set defined by the query. This method returns a system-generated
    * client listener object that backs your continuous query listener. To stop receiving notifications call the
    * {@link RemoteCache#removeClientListener} method using the returned client listener as argument.
    *
    * @param remoteCache   the remote cache to attach the continuous query listener
    * @param queryListener the continuous query listener instance
    * @param query         the query to be used for determining the matching set
    * @return the reference to a client listener object to be used for removing the continuous query listener
    */
   public static Object addContinuousQueryListener(RemoteCache<?, ?> remoteCache, ContinuousQueryListener queryListener, Query query) {
      SerializationContext serCtx = ProtoStreamMarshaller.getSerializationContext(remoteCache.getRemoteCacheManager());
      ClientEntryListener eventListener = new ClientEntryListener(serCtx, queryListener);
      Object[] factoryParams = makeFactoryParams(query);
      remoteCache.addClientListener(eventListener, factoryParams, null);
      return eventListener;
   }

   @ClientListener(filterFactoryName = Filters.CONTINUOUS_QUERY_FILTER_FACTORY_NAME,
         converterFactoryName = Filters.CONTINUOUS_QUERY_FILTER_FACTORY_NAME,
         useRawData = true, includeCurrentState = true)
   private static final class ClientEntryListener {

      private final SerializationContext serializationContext;

      private final ContinuousQueryListener queryListener;

      public ClientEntryListener(SerializationContext serializationContext, ContinuousQueryListener queryListener) {
         this.serializationContext = serializationContext;
         this.queryListener = queryListener;
      }

      @ClientCacheEntryCreated
      @ClientCacheEntryModified
      @ClientCacheEntryRemoved
      @ClientCacheEntryExpired
      public void handleClientCacheEntryCreatedEvent(ClientCacheEntryCustomEvent<byte[]> event) throws IOException {
         ContinuousQueryResult cqr = ProtobufUtil.fromByteArray(serializationContext, event.getEventData(), ContinuousQueryResult.class);
         Object key = ProtobufUtil.fromWrappedByteArray(serializationContext, cqr.getKey());
         Object value = cqr.getValue() != null ? ProtobufUtil.fromWrappedByteArray(serializationContext, cqr.getValue()) : cqr.getProjection();
         if (cqr.isJoining()) {
            queryListener.resultJoining(key, value);
         } else {
            queryListener.resultLeaving(key);
         }
      }
   }
}
