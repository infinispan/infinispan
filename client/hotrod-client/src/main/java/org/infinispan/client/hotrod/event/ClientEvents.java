package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.filter.Filters.makeFactoryParams;
import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.filter.Filters;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.util.ReflectionUtil;

public class ClientEvents {

   private ClientEvents() {
      // Static helper class, cannot be constructed
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
   public static void addClientQueryListener(RemoteCache<?, ?> remoteCache, Object listener, Query<?> query) {
      ClientListener l = ReflectionUtil.getAnnotation(listener.getClass(), ClientListener.class);
      if (l == null) {
         throw HOTROD.missingClientListenerAnnotation(listener.getClass().getName());
      }
      if (!l.useRawData()) {
         throw HOTROD.clientListenerMustUseRawData(listener.getClass().getName());
      }
      if (!l.filterFactoryName().equals(Filters.QUERY_DSL_FILTER_FACTORY_NAME)) {
         throw HOTROD.clientListenerMustUseDesignatedFilterConverterFactory(Filters.QUERY_DSL_FILTER_FACTORY_NAME);
      }
      if (!l.converterFactoryName().equals(Filters.QUERY_DSL_FILTER_FACTORY_NAME)) {
         throw HOTROD.clientListenerMustUseDesignatedFilterConverterFactory(Filters.QUERY_DSL_FILTER_FACTORY_NAME);
      }
      Object[] factoryParams = makeFactoryParams(query);
      remoteCache.addClientListener(listener, factoryParams, null);
   }
}
