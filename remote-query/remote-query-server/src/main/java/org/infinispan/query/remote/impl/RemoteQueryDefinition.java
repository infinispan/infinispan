package org.infinispan.query.remote.impl;

import static org.infinispan.query.remote.impl.ExternalizerIds.REMOTE_QUERY_DEFINITION;

import java.util.Collections;
import java.util.Set;

import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.AdvancedCache;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.impl.QueryDefinition;

public final class RemoteQueryDefinition extends QueryDefinition {

   RemoteQueryDefinition(String queryString) {
      super(queryString);
   }

   RemoteQueryDefinition(HSQuery query) {
      super(query);
   }

   @Override
   protected QueryEngine getQueryEngine(AdvancedCache<?, ?> cache) {
      return cache.getComponentRegistry().getComponent(RemoteQueryManager.class).getQueryEngine(cache);
   }

   public static final class Externalizer extends QueryDefinition.Externalizer {

      @Override
      public Set<Class<? extends QueryDefinition>> getTypeClasses() {
         return Collections.singleton(RemoteQueryDefinition.class);
      }

      @Override
      public Integer getId() {
         return REMOTE_QUERY_DEFINITION;
      }

      @Override
      protected QueryDefinition createQueryDefinition(String queryString) {
         return new RemoteQueryDefinition(queryString);
      }

      @Override
      protected QueryDefinition createQueryDefinition(HSQuery hsQuery) {
         return new RemoteQueryDefinition(hsQuery);
      }
   }
}
