package org.infinispan.search.mapper.mapping.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;
import org.infinispan.search.mapper.session.impl.InfinispanIndexedTypeContext;
import org.infinispan.search.mapper.session.impl.InfinispanTypeContextProvider;

class InfinispanTypeContextContainer implements InfinispanTypeContextProvider {

   // Use a LinkedHashMap for deterministic iteration
   private final Map<String, InfinispanIndexedTypeContextImpl<?>> indexedTypeContextsByEntityName = new LinkedHashMap<>();
   private final Map<Class<?>, InfinispanIndexedTypeContextImpl<?>> indexedTypeContextsByJavaType = new LinkedHashMap<>();

   private InfinispanTypeContextContainer(Builder builder) {
      for (InfinispanIndexedTypeContextImpl.Builder<?> contextBuilder : builder.indexedTypeContextBuilders) {
         InfinispanIndexedTypeContextImpl<?> indexedTypeContext = contextBuilder.build();
         indexedTypeContextsByEntityName.put(indexedTypeContext.getEntityName(), indexedTypeContext);
         indexedTypeContextsByJavaType.put(indexedTypeContext.getTypeIdentifier().javaClass(), indexedTypeContext);
      }
   }

   @Override
   public InfinispanIndexedTypeContext<?> getTypeContextByEntityType(Class<?> entityType) {
      return indexedTypeContextsByJavaType.get(entityType);
   }

   @Override
   public InfinispanIndexedTypeContext<?> getTypeContextByEntityName(String indexName) {
      return indexedTypeContextsByEntityName.get(indexName);
   }

   @Override
   public Map<String, Class<?>> getEntityClassByEntityName() {
      Map<String, InfinispanIndexedTypeContextImpl<?>> indexedTypeContextsByEntityName = this.indexedTypeContextsByEntityName;
      return indexedTypeContextsByEntityName.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, w -> w.getValue().javaClass()));
   }

   @Override
   public Collection<PojoRawTypeIdentifier<?>> allTypeIdentifiers() {
      return indexedTypeContextsByEntityName.values().stream()
            .map(InfinispanIndexedTypeContextImpl::getTypeIdentifier).collect(Collectors.toList());
   }

   InfinispanIndexedTypeContextImpl<?> getIndexedByEntityType(Class<?> entityType) {
      return indexedTypeContextsByJavaType.get(entityType);
   }

   InfinispanIndexedTypeContextImpl<?> getIndexedByEntityName(String indexName) {
      return indexedTypeContextsByEntityName.get(indexName);
   }

   Collection<InfinispanIndexedTypeContextImpl<?>> getAllIndexed() {
      return indexedTypeContextsByJavaType.values();
   }

   static class Builder {

      private final List<InfinispanIndexedTypeContextImpl.Builder<?>> indexedTypeContextBuilders = new ArrayList<>();

      Builder() {
      }

      <E> InfinispanIndexedTypeContextImpl.Builder<E> addIndexed(PojoRawTypeModel<E> typeModel, String entityName) {
         InfinispanIndexedTypeContextImpl.Builder<E> builder =
               new InfinispanIndexedTypeContextImpl.Builder<>(typeModel.typeIdentifier(), entityName);
         indexedTypeContextBuilders.add(builder);
         return builder;
      }

      InfinispanTypeContextContainer build() {
         return new InfinispanTypeContextContainer(this);
      }
   }
}
