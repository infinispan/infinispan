package org.infinispan.search.mapper.session.impl;

import java.util.Collection;

import org.hibernate.search.engine.backend.common.DocumentReference;
import org.hibernate.search.engine.backend.common.spi.DocumentReferenceConverter;
import org.hibernate.search.engine.search.query.dsl.SearchQuerySelectStep;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionLoadingContext;
import org.hibernate.search.mapper.pojo.session.spi.AbstractPojoSearchSession;
import org.hibernate.search.mapper.pojo.work.spi.PojoIndexer;
import org.hibernate.search.util.common.AssertionFailure;
import org.infinispan.search.mapper.common.EntityReference;
import org.infinispan.search.mapper.common.impl.EntityReferenceImpl;
import org.infinispan.search.mapper.model.impl.InfinispanRuntimeIntrospector;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.scope.impl.SearchScopeImpl;
import org.infinispan.search.mapper.search.loading.context.impl.InfinispanLoadingContext;
import org.infinispan.search.mapper.session.SearchSession;

/**
 * @author Fabio Massimo Ercoli
 */
public class InfinispanSearchSession extends AbstractPojoSearchSession implements SearchSession,
      DocumentReferenceConverter<EntityReference> {

   private final InfinispanSearchSessionMappingContext mappingContext;
   private final PojoSelectionEntityLoader<?> entityLoader;
   private final InfinispanTypeContextProvider typeContextProvider;

   public InfinispanSearchSession(InfinispanSearchSessionMappingContext mappingContext,
                                  PojoSelectionEntityLoader<?> entityLoader, InfinispanTypeContextProvider typeContextProvider) {
      super(mappingContext);
      this.mappingContext = mappingContext;
      this.entityLoader = entityLoader;
      this.typeContextProvider = typeContextProvider;
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
   public EntityReference fromDocumentReference(DocumentReference reference) {
      InfinispanIndexedTypeContext<?> typeContext =
            typeContextProvider.indexedForEntityName(reference.typeName());
      if (typeContext == null) {
         throw new AssertionFailure("Document reference " + reference + " refers to an unknown index");
      }
      Object id = typeContext.identifierMapping().fromDocumentIdentifier(reference.id(), this);
      return new EntityReferenceImpl(typeContext.typeIdentifier(), typeContext.name(), id);
   }

   @Override
   public PojoIndexer createIndexer() {
      return super.createIndexer();
   }

   private <E> SearchQuerySelectStep<?, EntityReference, E, ?, ?, ?> search(SearchScopeImpl<E> scope) {
      return scope.search(this, this);
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
}
