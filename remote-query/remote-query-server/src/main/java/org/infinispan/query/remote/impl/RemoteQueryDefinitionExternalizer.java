package org.infinispan.query.remote.impl;

import static org.infinispan.query.remote.impl.ExternalizerIds.REMOTE_QUERY_DEFINITION;

import java.util.Collections;
import java.util.Set;

import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.query.clustered.AbstractQueryDefinitionExternalizer;

/**
 * @since 9.2
 */
public class RemoteQueryDefinitionExternalizer extends AbstractQueryDefinitionExternalizer<RemoteQueryDefinition> {

   @Override
   public Set<Class<? extends RemoteQueryDefinition>> getTypeClasses() {
      return Collections.singleton(RemoteQueryDefinition.class);
   }

   @Override
   public Integer getId() {
      return REMOTE_QUERY_DEFINITION;
   }

   @Override
   protected RemoteQueryDefinition createQueryDefinition(String q) {
      return new RemoteQueryDefinition(q);
   }

   @Override
   protected RemoteQueryDefinition createQueryDefinition(HSQuery hsQuery) {
      return new RemoteQueryDefinition(hsQuery);
   }

}
