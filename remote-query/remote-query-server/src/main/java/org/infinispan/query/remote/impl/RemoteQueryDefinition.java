package org.infinispan.query.remote.impl;

import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.AdvancedCache;
import org.infinispan.query.QueryDefinition;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;

public class RemoteQueryDefinition extends QueryDefinition {

   public RemoteQueryDefinition(String queryString) {
      super(queryString);
   }

   public RemoteQueryDefinition(HSQuery query) {
      super(query);
   }

   @Override
   protected QueryEngine getQueryEngine(AdvancedCache<?, ?> cache) {
      return cache.getComponentRegistry().getComponent(RemoteQueryManager.class).getQueryEngine(cache);
   }
}
