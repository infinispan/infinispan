package org.infinispan.query.mapper.mapping.impl;

import org.hibernate.search.engine.backend.index.IndexManager;
import org.hibernate.search.engine.mapper.mapping.spi.MappedIndexManager;
import org.hibernate.search.engine.search.projection.spi.ProjectionMappedTypeContext;
import org.hibernate.search.mapper.pojo.identity.spi.IdentifierMapping;
import org.hibernate.search.mapper.pojo.loading.definition.spi.PojoEntityLoadingBindingContext;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoIndexedTypeExtendedMappingCollector;
import org.hibernate.search.mapper.pojo.model.path.spi.PojoPathFilter;
import org.hibernate.search.mapper.pojo.model.spi.PojoPropertyModel;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.infinispan.query.mapper.mapping.SearchIndexedEntity;
import org.infinispan.query.mapper.search.loading.context.impl.InfinispanSelectionLoadingBinder;
import org.infinispan.query.mapper.search.loading.context.impl.InfinispanSelectionLoadingStrategy;
import org.infinispan.query.mapper.session.impl.InfinispanIndexedTypeContext;

class InfinispanIndexedTypeContextImpl<E> implements SearchIndexedEntity, ProjectionMappedTypeContext,
      InfinispanIndexedTypeContext<E> {

   private final PojoRawTypeIdentifier<E> typeIdentifier;
   private final String entityName;
   private final IdentifierMapping identifierMapping;
   private final MappedIndexManager indexManager;

   private InfinispanIndexedTypeContextImpl(Builder<E> builder) {
      this.typeIdentifier = builder.typeIdentifier;
      this.entityName = builder.entityName;
      this.identifierMapping = builder.identifierMapping;
      this.indexManager = builder.indexManager;
   }

   @Override
   public PojoRawTypeIdentifier<E> typeIdentifier() {
      return typeIdentifier;
   }

   @Override
   public String name() {
      return entityName;
   }

   @Override
   public IdentifierMapping identifierMapping() {
      return identifierMapping;
   }

   @Override
   public Class<?> javaClass() {
      return typeIdentifier.javaClass();
   }

   @Override
   public boolean loadingAvailable() {
      return true;
   }

   @Override
   public IndexManager indexManager() {
      return indexManager.toAPI();
   }

   static class Builder<E> implements PojoIndexedTypeExtendedMappingCollector {
      private final PojoRawTypeIdentifier<E> typeIdentifier;
      private final String entityName;
      private IdentifierMapping identifierMapping;
      private MappedIndexManager indexManager;
      private PojoPathFilter dirtyFilter;
      private InfinispanSelectionLoadingStrategy loadingStrategy;

      Builder(PojoRawTypeIdentifier<E> typeIdentifier, String entityName) {
         this.typeIdentifier = typeIdentifier;
         this.entityName = entityName;
      }

      @Override
      public void documentIdSourceProperty(PojoPropertyModel<?> documentIdSourceProperty) {
         // Nothing to do
      }

      @Override
      public void identifierMapping(IdentifierMapping identifierMapping) {
         this.identifierMapping = identifierMapping;
      }

      @Override
      public void dirtyFilter(PojoPathFilter dirtyFilter) {
         // Nothing to do
      }

      @Override
      public void applyLoadingBinder(Object binder, PojoEntityLoadingBindingContext context) {
         var castBinder = (InfinispanSelectionLoadingBinder) binder;
         this.loadingStrategy = castBinder.createLoadingStrategy();
         if ( this.loadingStrategy != null ) {
            context.selectionLoadingStrategy( typeIdentifier.javaClass(), this.loadingStrategy );
         }
      }

      @Override
      public void indexManager(MappedIndexManager indexManager) {
         this.indexManager = indexManager;
      }

      InfinispanIndexedTypeContextImpl<E> build() {
         return new InfinispanIndexedTypeContextImpl<>(this);
      }
   }
}
