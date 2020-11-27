package org.infinispan.search.mapper.mapping.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;
import org.infinispan.search.mapper.session.impl.InfinispanTypeContextProvider;

class InfinispanTypeContextContainer implements InfinispanTypeContextProvider {

   // Use a LinkedHashMap for deterministic iteration
   private final Map<String, InfinispanIndexedTypeContextImpl<?>> indexedTypeContextsByEntityName = new LinkedHashMap<>();
   private final Map<Class<?>, InfinispanIndexedTypeContextImpl<?>> indexedTypeContextsByJavaType = new LinkedHashMap<>();

   private InfinispanTypeContextContainer(Builder builder) {
      for (InfinispanIndexedTypeContextImpl.Builder<?> contextBuilder : builder.indexedTypeContextBuilders) {
         InfinispanIndexedTypeContextImpl<?> indexedTypeContext = contextBuilder.build();
         indexedTypeContextsByEntityName.put(indexedTypeContext.name(), indexedTypeContext);
         if (!indexedTypeContext.typeIdentifier().isNamed()) {
            // If the type is named the java class is byte[] or ProtobufValueWrapper
            // and multiple types may match that class, so we don't add it here.
            indexedTypeContextsByJavaType.put(indexedTypeContext.typeIdentifier().javaClass(), indexedTypeContext);
         }
      }
   }

   @Override
   @SuppressWarnings("unchecked")
   public <E> InfinispanIndexedTypeContextImpl<E> indexedForExactType(Class<E> entityType) {
      return (InfinispanIndexedTypeContextImpl<E>) indexedTypeContextsByJavaType.get(entityType);
   }

   @Override
   public InfinispanIndexedTypeContextImpl<?> indexedForEntityName(String indexName) {
      return indexedTypeContextsByEntityName.get(indexName);
   }

   @Override
   public Collection<PojoRawTypeIdentifier<?>> allTypeIdentifiers() {
      return indexedTypeContextsByEntityName.values().stream()
            .map(InfinispanIndexedTypeContextImpl::typeIdentifier).collect(Collectors.toList());
   }

   Collection<InfinispanIndexedTypeContextImpl<?>> allIndexed() {
      return indexedTypeContextsByEntityName.values();
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
