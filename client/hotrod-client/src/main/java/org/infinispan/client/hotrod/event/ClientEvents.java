package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.filter.Filters.makeFactoryParams;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.filter.Filters;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.query.dsl.Query;

public class ClientEvents {

   private static final Log log = LogFactory.getLog(ClientEvents.class, Log.class);

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
}
