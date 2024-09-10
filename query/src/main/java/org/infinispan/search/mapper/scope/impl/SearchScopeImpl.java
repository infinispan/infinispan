package org.infinispan.search.mapper.scope.impl;

import org.hibernate.search.engine.backend.mapping.spi.BackendMappingContext;
import org.hibernate.search.engine.backend.session.spi.DetachedBackendSessionContext;
import org.hibernate.search.engine.common.EntityReference;
import org.hibernate.search.engine.search.aggregation.dsl.SearchAggregationFactory;
import org.hibernate.search.engine.search.predicate.dsl.SearchPredicateFactory;
import org.hibernate.search.engine.search.projection.dsl.SearchProjectionFactory;
import org.hibernate.search.engine.search.query.dsl.SearchQuerySelectStep;
import org.hibernate.search.engine.search.sort.dsl.SearchSortFactory;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.scope.spi.PojoScopeDelegate;
import org.hibernate.search.mapper.pojo.scope.spi.PojoScopeSessionContext;
import org.infinispan.query.impl.EntityLoaderFactory;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.scope.SearchWorkspace;

public class SearchScopeImpl<E> implements SearchScope<E> {

   private final BackendMappingContext mappingContext;
   private final PojoScopeDelegate<EntityReference, E, PojoRawTypeIdentifier<? extends E>> delegate;
   private final EntityLoaderFactory<E> entityLoader;

   public SearchScopeImpl(BackendMappingContext mappingContext,
                          PojoScopeDelegate<EntityReference, E, PojoRawTypeIdentifier<? extends E>> delegate,
                          EntityLoaderFactory<E> entityLoader) {
      this.mappingContext = mappingContext;
      this.delegate = delegate;
      this.entityLoader = entityLoader;
   }

   @Override
   public SearchPredicateFactory predicate() {
      return delegate.predicate();
   }

   @Override
   public SearchSortFactory sort() {
      return delegate.sort();
   }

   @Override
   public SearchProjectionFactory<EntityReference, E> projection() {
      return delegate.projection();
   }

   @Override
   public SearchAggregationFactory aggregation() {
      return delegate.aggregation();
   }

   @Override
   public SearchWorkspace workspace() {
      return new SearchWorkspaceImpl(delegate.workspace(DetachedBackendSessionContext.of(mappingContext, null)));
   }

   public SearchQuerySelectStep<?, EntityReference, E, ?, ?, ?> search(PojoScopeSessionContext sessionContext) {
      return delegate.search(sessionContext, entityLoader.builder());
   }
}
