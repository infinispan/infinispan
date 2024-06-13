package org.infinispan.search.mapper.work.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hibernate.search.engine.backend.work.execution.DocumentCommitStrategy;
import org.hibernate.search.engine.backend.work.execution.DocumentRefreshStrategy;
import org.hibernate.search.engine.backend.work.execution.OperationSubmitter;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.route.DocumentRoutesDescriptor;
import org.hibernate.search.mapper.pojo.work.spi.PojoIndexer;
import org.infinispan.commons.util.concurrent.CompletableFutures;
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
   private final BlockingManager blockingManager;

   public SearchIndexerImpl(PojoIndexer delegate, EntityConverter entityConverter,
                            InfinispanTypeContextProvider typeContextProvider, BlockingManager blockingManager) {
      this.delegate = delegate;
      this.entityConverter = entityConverter;
      this.typeContextProvider = typeContextProvider;
      this.blockingManager = blockingManager;
   }

   @Override
   public CompletableFuture<?> add(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity, providedId);
      if (convertedValue == null) {
         return CompletableFutures.completedNull();
      }

      try {
         return delegate.add(convertedValue.typeIdentifier, providedId,
               DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
               DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
               OperationSubmitter.rejecting());
      } catch (Exception ex) {
         return blockingManager.runBlocking(() -> delegate.add(convertedValue.typeIdentifier, providedId,
               DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
               DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
               OperationSubmitter.blocking()), this).toCompletableFuture();
      }
   }

   @Override
   public CompletableFuture<?> addOrUpdate(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity, providedId);
      if (convertedValue == null) {
         return CompletableFutures.completedNull();
      }

      try {
         return delegate.addOrUpdate(convertedValue.typeIdentifier, providedId,
               DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
               DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
               OperationSubmitter.rejecting());
      } catch (Exception ex) {
         return blockingManager.runBlocking(() -> delegate.addOrUpdate(convertedValue.typeIdentifier, providedId,
               DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
               DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
               OperationSubmitter.blocking()), this).toCompletableFuture();
      }
   }

   @Override
   public CompletableFuture<?> delete(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity, providedId);
      if (convertedValue == null) {
         return CompletableFutures.completedNull();
      }

      try {
         return delegate.delete(convertedValue.typeIdentifier, providedId,
               DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
               DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
               OperationSubmitter.rejecting());
      } catch (Exception ex) {
         return blockingManager.runBlocking(() -> delegate.delete(convertedValue.typeIdentifier, providedId,
               DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
               DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
               OperationSubmitter.blocking()), this).toCompletableFuture();
      }
   }

   @Override
   public CompletableFuture<?> purge(Object providedId, String routingKey) {
      AtomicBoolean full = new AtomicBoolean(false);
      return CompletableFuture.allOf(typeContextProvider.allTypeIdentifiers().stream()
            .map((typeIdentifier) -> {
               if (full.get()) {
                  return blockingManager.runBlocking(() -> delegate.delete(typeIdentifier, providedId,
                        DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey),
                        DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
                        OperationSubmitter.blocking()), this).toCompletableFuture();
               }

               try {
                  return delegate.delete(typeIdentifier, providedId,
                        DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey),
                        DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
                        OperationSubmitter.rejecting());
               } catch (Exception ex) {
                  full.set(true);

                  return blockingManager.runBlocking(() -> delegate.delete(typeIdentifier, providedId,
                        DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey),
                        DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
                        OperationSubmitter.blocking()), this).toCompletableFuture();
               }
            } )
            .toArray(CompletableFuture[]::new));
   }

   @Override
   public void close() {
   }

   private ConvertedValue convertedValue(Object entity, Object providedId) {
      if (entity == null) {
         return null;
      }

      if (entityConverter == null || !entityConverter.typeIsIndexed(entity.getClass())) {
         InfinispanIndexedTypeContext<?> typeContext = typeContextProvider.indexedForExactType(entity.getClass());
         if (typeContext == null) {
            return null;
         }

         return new ConvertedValue(typeContext, entity);
      }

      EntityConverter.ConvertedEntity converted = entityConverter.convert(entity, providedId);
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
      private final PojoRawTypeIdentifier<?> typeIdentifier;
      private final Object value;

      public ConvertedValue(InfinispanIndexedTypeContext<?> typeContext, Object value) {
         this.typeIdentifier = typeContext.typeIdentifier();
         this.value = value;
      }
   }
}
