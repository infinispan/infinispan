package org.infinispan.search.mapper.mapping.impl;

import org.hibernate.search.engine.backend.index.IndexManager;
import org.hibernate.search.engine.mapper.mapping.spi.MappedIndexManager;
import org.hibernate.search.mapper.pojo.identity.spi.IdentifierMapping;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoIndexedTypeExtendedMappingCollector;
import org.hibernate.search.mapper.pojo.model.path.spi.PojoPathFilter;
import org.hibernate.search.mapper.pojo.model.spi.PojoPropertyModel;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.infinispan.search.mapper.mapping.SearchIndexedEntity;
import org.infinispan.search.mapper.session.impl.InfinispanIndexedTypeContext;

class InfinispanIndexedTypeContextImpl<E> implements SearchIndexedEntity, InfinispanIndexedTypeContext<E> {

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
   public IndexManager indexManager() {
      return indexManager.toAPI();
   }

   static class Builder<E> implements PojoIndexedTypeExtendedMappingCollector {
      private final PojoRawTypeIdentifier<E> typeIdentifier;
      private final String entityName;
      private IdentifierMapping identifierMapping;
      private MappedIndexManager indexManager;
      private PojoPathFilter dirtyFilter;

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
      public void indexManager(MappedIndexManager indexManager) {
         this.indexManager = indexManager;
      }

      InfinispanIndexedTypeContextImpl<E> build() {
         return new InfinispanIndexedTypeContextImpl<>(this);
      }
   }
}
