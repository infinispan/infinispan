package org.infinispan.search.mapper.mapping.impl;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.hibernate.search.engine.backend.common.spi.EntityReferenceFactory;
import org.hibernate.search.engine.common.spi.SearchIntegration;
import org.hibernate.search.engine.environment.thread.spi.ThreadPoolProvider;
import org.hibernate.search.engine.reporting.FailureHandler;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.hibernate.search.mapper.pojo.mapping.spi.AbstractPojoMappingImplementor;
import org.hibernate.search.mapper.pojo.mapping.spi.PojoMappingDelegate;
import org.hibernate.search.mapper.pojo.massindexing.spi.PojoMassIndexerAgent;
import org.hibernate.search.mapper.pojo.massindexing.spi.PojoMassIndexerAgentCreateContext;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.scope.spi.PojoScopeDelegate;
import org.hibernate.search.util.common.AssertionFailure;
import org.hibernate.search.util.common.impl.Closer;
import org.hibernate.search.util.common.logging.impl.LoggerFactory;
import org.infinispan.search.mapper.common.EntityReference;
import org.infinispan.search.mapper.common.impl.EntityReferenceImpl;
import org.infinispan.search.mapper.log.impl.Log;
import org.infinispan.search.mapper.mapping.EntityConverter;
import org.infinispan.search.mapper.mapping.SearchIndexedEntity;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.scope.impl.SearchScopeImpl;
import org.infinispan.search.mapper.session.SearchSession;
import org.infinispan.search.mapper.session.impl.InfinispanIndexedTypeContext;
import org.infinispan.search.mapper.session.impl.InfinispanSearchSession;
import org.infinispan.search.mapper.session.impl.InfinispanSearchSessionMappingContext;
import org.infinispan.search.mapper.work.SearchIndexer;
import org.infinispan.search.mapper.work.impl.SearchIndexerImpl;

public class InfinispanMapping extends AbstractPojoMappingImplementor<SearchMapping>
      implements SearchMapping, InfinispanSearchSessionMappingContext, EntityReferenceFactory<EntityReference> {

   private static final Log log = LoggerFactory.make(Log.class, MethodHandles.lookup());

   private final InfinispanTypeContextContainer typeContextContainer;
   private final PojoSelectionEntityLoader<?> entityLoader;
   private final EntityConverter entityConverter;
   private final SearchSession mappingSession;
   private final SearchIndexer searchIndexer;

   private final Set<String> allIndexedEntityNames;
   private final Set<Class<?>> allIndexedEntityJavaClasses;

   private SearchIntegration integration;
   private boolean close = false;

   InfinispanMapping(PojoMappingDelegate mappingDelegate, InfinispanTypeContextContainer typeContextContainer,
                     PojoSelectionEntityLoader<?> entityLoader, EntityConverter entityConverter) {
      super(mappingDelegate);
      this.typeContextContainer = typeContextContainer;
      this.entityLoader = entityLoader;
      this.entityConverter = entityConverter;
      this.mappingSession = new InfinispanSearchSession(this, entityLoader, typeContextContainer);
      this.searchIndexer = new SearchIndexerImpl(mappingSession.createIndexer(), entityConverter, typeContextContainer,
            // TODO: need to propagate these objects down
            null, null);
      this.allIndexedEntityNames = typeContextContainer.allIndexed().stream()
            .map(SearchIndexedEntity::name).collect(Collectors.toSet());
      this.allIndexedEntityJavaClasses = typeContextContainer.allIndexed().stream()
            .map(SearchIndexedEntity::javaClass).collect(Collectors.toSet());
   }

   @Override
   public void close() {
      try (Closer<RuntimeException> closer = new Closer<>()) {
         closer.push(SearchIntegration::close, integration);
         close = true;
      }
   }

   @Override
   public boolean isClose() {
      return close;
   }

   @Override
   public <E> SearchScope<E> scope(Collection<? extends Class<? extends E>> targetedTypes) {
      return createScope(targetedTypes);
   }

   @Override
   public SearchScope<?> scopeAll() {
      return doCreateScope(typeContextContainer.allTypeIdentifiers());
   }

   @Override
   public SearchSession getMappingSession() {
      return mappingSession;
   }

   @Override
   public SearchIndexer getSearchIndexer() {
      return searchIndexer;
   }
   @Override
   public SearchIndexedEntity indexedEntity(Class<?> entityType) {
      return typeContextContainer.indexedForExactType(entityType);
   }

   @Override
   public SearchIndexedEntity indexedEntity(String entityName) {
      return typeContextContainer.indexedForEntityName(entityName);
   }

   @Override
   public Collection<? extends SearchIndexedEntity> allIndexedEntities() {
      return typeContextContainer.allIndexed();
   }

   @Override
   public Set<String> allIndexedEntityNames() {
      return allIndexedEntityNames;
   }

   @Override
   public Set<Class<?>> allIndexedEntityJavaClasses() {
      return allIndexedEntityJavaClasses;
   }

   @Override
   public Class<?> toConvertedEntityJavaClass(Object value) {
      if (value == null) {
         return null;
      }
      Class<?> c = value.getClass();
      if (entityConverter != null && c.equals(entityConverter.targetType())) {
         return entityConverter.convertedTypeIdentifier().javaClass();
      } else {
         return c;
      }
   }

   @Override
   public FailureHandler getFailureHandler() {
      return delegate().failureHandler();
   }

   @Override
   @SuppressWarnings("unchecked")
   public <E> SearchScopeImpl<E> createScope(Collection<? extends Class<? extends E>> classes) {
      Class<?> converterTargetType = entityConverter == null ? null : entityConverter.targetType();
      List<PojoRawTypeIdentifier<? extends E>> typeIdentifiers = new ArrayList<>(classes.size());
      for (Class<? extends E> clazz : classes) {
         if (clazz.equals(converterTargetType)) {
            // Include all protobuf types
            typeIdentifiers.add((PojoRawTypeIdentifier<? extends E>) entityConverter.convertedTypeIdentifier());
         } else {
            typeIdentifiers.add(PojoRawTypeIdentifier.of(clazz));
         }
      }
      return doCreateScope(typeIdentifiers);
   }

   @Override
   public <E> SearchScopeImpl<E> createScope(Class<E> expectedSuperType, Collection<String> entityNames) {
      List<PojoRawTypeIdentifier<? extends E>> typeIdentifiers = new ArrayList<>(entityNames.size());
      for (String entityName : entityNames) {
         typeIdentifiers.add(entityTypeIdentifier(expectedSuperType, entityName));
      }
      return doCreateScope(typeIdentifiers);
   }

   @Override
   public SearchMapping toConcreteType() {
      return this;
   }

   @Override
   public EntityReferenceFactory<EntityReference> entityReferenceFactory() {
      return this;
   }

   @Override
   public ThreadPoolProvider threadPoolProvider() {
      return delegate().threadPoolProvider();
   }

   @Override
   public FailureHandler failureHandler() {
      return delegate().failureHandler();
   }

   @Override
   public PojoMassIndexerAgent createMassIndexerAgent(PojoMassIndexerAgentCreateContext context) {
      // No coordination: we don't prevent automatic indexing from continuing while mass indexing.
      return PojoMassIndexerAgent.noOp();
   }

   @Override
   public EntityReference createEntityReference(String typeName, Object identifier) {
      InfinispanIndexedTypeContext<?> typeContext = typeContextContainer.indexedForEntityName(typeName);
      if (typeContext == null) {
         throw new AssertionFailure(
               "Type name " + typeName + " refers to an unknown type"
         );
      }
      return new EntityReferenceImpl(typeContext.typeIdentifier(), typeContext.name(), identifier);
   }

   public SearchIntegration getIntegration() {
      return integration;
   }

   public void setIntegration(SearchIntegration integration) {
      this.integration = integration;
   }

   private <E> SearchScopeImpl<E> doCreateScope(Collection<PojoRawTypeIdentifier<? extends E>> typeIdentifiers) {
      PojoScopeDelegate<EntityReference, E, PojoRawTypeIdentifier<? extends E>> pojoScopeDelegate =
            delegate().createPojoScope(this, typeIdentifiers,
                  // Store the type identifier as additional metadata
                  typeIdentifier -> typeIdentifier);
      return new SearchScopeImpl(this, pojoScopeDelegate, this.entityLoader);
   }

   private <T> PojoRawTypeIdentifier<? extends T> entityTypeIdentifier(Class<T> expectedSuperType, String entityName) {
      InfinispanIndexedTypeContext<?> typeContext = typeContextContainer.indexedForEntityName(entityName);
      if (typeContext == null) {
         throw log.invalidEntityName(entityName);
      }
      PojoRawTypeIdentifier<?> typeIdentifier = typeContext.typeIdentifier();
      Class<?> actualJavaType = typeIdentifier.javaClass();
      if (!expectedSuperType.isAssignableFrom(actualJavaType)) {
         throw log.invalidEntitySuperType(entityName, expectedSuperType, actualJavaType);
      }
      // The cast below is safe because we just checked above that the type extends "expectedSuperType", which extends T
      @SuppressWarnings("unchecked")
      PojoRawTypeIdentifier<? extends T> castedTypeIdentifier = (PojoRawTypeIdentifier<? extends T>) typeIdentifier;
      return castedTypeIdentifier;
   }
}
