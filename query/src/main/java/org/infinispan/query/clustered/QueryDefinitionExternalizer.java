package org.infinispan.query.clustered;

import java.util.Collections;
import java.util.Set;

import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.query.QueryDefinition;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

/**
 * @since 9.2
 */
public class QueryDefinitionExternalizer extends AbstractQueryDefinitionExternalizer<QueryDefinition> {

   @Override
   public Set<Class<? extends QueryDefinition>> getTypeClasses() {
      return Collections.singleton(QueryDefinition.class);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.QUERY_DEFINITION;
   }

   @Override
   protected QueryDefinition createQueryDefinition(String q) {
      return new QueryDefinition(q);
   }

   @Override
   protected QueryDefinition createQueryDefinition(HSQuery hsQuery) {
      return new QueryDefinition(hsQuery);
   }
}
