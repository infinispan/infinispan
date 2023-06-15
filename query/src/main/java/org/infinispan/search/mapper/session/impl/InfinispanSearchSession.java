package org.infinispan.search.mapper.session.impl;

import java.util.Collection;

import org.hibernate.search.engine.common.EntityReference;
import org.hibernate.search.engine.search.query.dsl.SearchQuerySelectStep;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionLoadingContext;
import org.hibernate.search.mapper.pojo.session.spi.AbstractPojoSearchSession;
import org.hibernate.search.mapper.pojo.work.spi.ConfiguredSearchIndexingPlanFilter;
import org.hibernate.search.mapper.pojo.work.spi.PojoIndexer;
import org.infinispan.search.mapper.model.impl.InfinispanRuntimeIntrospector;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.scope.impl.SearchScopeImpl;
import org.infinispan.search.mapper.search.loading.context.impl.InfinispanLoadingContext;
import org.infinispan.search.mapper.session.SearchSession;

/**
 * @author Fabio Massimo Ercoli
 */
public class InfinispanSearchSession extends AbstractPojoSearchSession implements SearchSession {

   private static final ConfiguredSearchIndexingPlanFilter ACCEPT_ALL = typeIdentifier -> true;

   private final InfinispanSearchSessionMappingContext mappingContext;
   private final PojoSelectionEntityLoader<?> entityLoader;

   public InfinispanSearchSession(InfinispanSearchSessionMappingContext mappingContext,
                                  PojoSelectionEntityLoader<?> entityLoader) {
      super(mappingContext);
      this.mappingContext = mappingContext;
      this.entityLoader = entityLoader;
   }

   @Override
   public void close() {
      // Nothing to do
   }

   @Override
   public <E> SearchQuerySelectStep<?, EntityReference, E, ?, ?, ?> search(SearchScope<E> scope) {
      return search((SearchScopeImpl<E>) scope);
   }

   public <E> SearchScopeImpl<E> scope(Collection<? extends Class<? extends E>> types) {
      return mappingContext.createScope(types);
   }

   @Override
   public <T> SearchScope<T> scope(Class<T> expectedSuperType, Collection<String> entityNames) {
      return mappingContext.createScope(expectedSuperType, entityNames);
   }

   @Override
   public PojoIndexer createIndexer() {
      return super.createIndexer();
   }

   private <E> SearchQuerySelectStep<?, EntityReference, E, ?, ?, ?> search(SearchScopeImpl<E> scope) {
      return scope.search(this);
   }

   @Override
   public String tenantIdentifier() {
      // tenant is not used by ISPN
      return null;
   }

   @Override
   public InfinispanRuntimeIntrospector runtimeIntrospector() {
      return new InfinispanRuntimeIntrospector();
   }

   @Override
   public PojoSelectionLoadingContext defaultLoadingContext() {
      return new InfinispanLoadingContext.Builder<>(entityLoader).build();
   }

   @Override
   public ConfiguredSearchIndexingPlanFilter configuredIndexingPlanFilter() {
      return ACCEPT_ALL;
   }
}
