package org.infinispan.search.mapper.work.impl;

import java.util.concurrent.CompletableFuture;

import org.hibernate.search.engine.backend.work.execution.DocumentCommitStrategy;
import org.hibernate.search.engine.backend.work.execution.DocumentRefreshStrategy;
import org.hibernate.search.engine.backend.work.execution.OperationSubmitter;
import org.hibernate.search.engine.common.execution.spi.SimpleScheduledExecutor;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.route.DocumentRoutesDescriptor;
import org.hibernate.search.mapper.pojo.work.spi.PojoIndexer;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.query.concurrent.InfinispanIndexingExecutorProvider;
import org.infinispan.search.mapper.mapping.EntityConverter;
import org.infinispan.search.mapper.session.impl.InfinispanIndexedTypeContext;
import org.infinispan.search.mapper.session.impl.InfinispanTypeContextProvider;
import org.infinispan.search.mapper.work.SearchIndexer;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * @author Fabio Massimo Ercoli
 */
public class SearchIndexerImpl implements SearchIndexer {

   private final PojoIndexer delegate;
   private final EntityConverter entityConverter;
   private final InfinispanTypeContextProvider typeContextProvider;

   private final SimpleScheduledExecutor offloadingExecutor;

   public SearchIndexerImpl(PojoIndexer delegate, EntityConverter entityConverter,
         InfinispanTypeContextProvider typeContextProvider, BlockingManager blockingManager) {
      this.delegate = delegate;
      this.entityConverter = entityConverter;
      this.typeContextProvider = typeContextProvider;
      this.offloadingExecutor = InfinispanIndexingExecutorProvider.writeExecutor(blockingManager);
   }

   @Override
   public CompletableFuture<?> add(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity);
      if (convertedValue == null) {
         return CompletableFutures.completedNull();
      }

      return delegate.add(convertedValue.typeIdentifier, providedId,
            DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
            DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
            OperationSubmitter.offloading(offloadingExecutor::submit));
   }

   @Override
   public CompletableFuture<?> addOrUpdate(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity);
      if (convertedValue == null) {
         return CompletableFutures.completedNull();
      }

      return delegate.addOrUpdate(convertedValue.typeIdentifier, providedId,
            DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
            DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
            OperationSubmitter.offloading(offloadingExecutor::submit));
   }

   @Override
   public CompletableFuture<?> delete(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity);
      if (convertedValue == null) {
         return CompletableFutures.completedNull();
      }

      return delegate.delete(convertedValue.typeIdentifier, providedId,
            DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
            DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
            OperationSubmitter.offloading(offloadingExecutor::submit));
   }

   @Override
   public CompletableFuture<?> purge(Object providedId, String routingKey) {
      return CompletableFuture.allOf(typeContextProvider.allTypeIdentifiers().stream()
            .map((typeIdentifier) -> delegate.delete(typeIdentifier, providedId,
                  DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey),
                  DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
                  OperationSubmitter.offloading(offloadingExecutor::submit)))
            .toArray(CompletableFuture[]::new));
   }

   @Override
   public void close() {
   }

   private ConvertedValue convertedValue(Object entity) {
      if (entity == null) {
         return null;
      }

      if (entityConverter == null || !entity.getClass().equals(entityConverter.targetType())) {
         InfinispanIndexedTypeContext<?> typeContext = typeContextProvider.indexedForExactType(entity.getClass());
         if (typeContext == null) {
            return null;
         }

         return new ConvertedValue(typeContext, entity);
      }

      EntityConverter.ConvertedEntity converted = entityConverter.convert(entity);
      if (converted.skip()) {
         return null;
      }

      InfinispanIndexedTypeContext<?> typeContext = typeContextProvider.indexedForEntityName(converted.entityName());
      if (typeContext == null) {
         return null;
      }

      return new ConvertedValue(typeContext, converted.value());
   }

   private static class ConvertedValue {
      private PojoRawTypeIdentifier<?> typeIdentifier;
      private Object value;

      public ConvertedValue(InfinispanIndexedTypeContext<?> typeContext, Object value) {
         this.typeIdentifier = typeContext.typeIdentifier();
         this.value = value;
      }
   }
}
