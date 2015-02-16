package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQuery;

public class ClientEvents {

   /**
    * The name of the factory used for query DSL based filters and converters. This factory is provided internally by
    * the server.
    */
   public static final String QUERY_DSL_FILTER_FACTORY_NAME = "query-dsl-filter-converter-factory";

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
    * ClientEvents#QUERY_DSL_FILTER_FACTORY_NAME}
    *
    * @param remoteCache the remote cache to attach the listener
    * @param listener    the listener instance
    * @param query       the query to be used for filtering and conversion (if projections are used)
    */
   public static void addClientQueryListener(RemoteCache<?, ?> remoteCache, Object listener, Query query) {
      ClientListener l = listener.getClass().getAnnotation(ClientListener.class);
      if (!l.useRawData()) {
         throw new IllegalArgumentException("The client listener must use raw data");
      }
      if (!l.filterFactoryName().equals(QUERY_DSL_FILTER_FACTORY_NAME)) {
         throw new IllegalArgumentException("The client listener must use the '" + QUERY_DSL_FILTER_FACTORY_NAME + "' filter factory");
      }
      if (!l.converterFactoryName().equals(QUERY_DSL_FILTER_FACTORY_NAME)) {
         throw new IllegalArgumentException("The client listener must use the '" + QUERY_DSL_FILTER_FACTORY_NAME + "' converter factory");
      }
      Object[] factoryParams = new Object[]{((BaseQuery) query).getJPAQuery()};
      remoteCache.addClientListener(listener, factoryParams, factoryParams);
   }
}
