package org.infinispan.search.mapper.work.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.work.spi.PojoIndexer;
import org.hibernate.search.util.common.logging.impl.LoggerFactory;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.search.mapper.log.impl.Log;
import org.infinispan.search.mapper.mapping.EntityConverter;
import org.infinispan.search.mapper.session.impl.InfinispanIndexedTypeContext;
import org.infinispan.search.mapper.session.impl.InfinispanTypeContextProvider;
import org.infinispan.search.mapper.work.SearchIndexer;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.NonBlockingManager;

import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * @author Fabio Massimo Ercoli
 */
public class SearchIndexerImpl implements SearchIndexer {

   private static final Log log = LoggerFactory.make(Log.class, MethodHandles.lookup());

   private final PojoIndexer delegate;
   private final EntityConverter entityConverter;
   private final InfinispanTypeContextProvider typeContextProvider;
   private final NonBlockingManager nonBlockingManager;

   private final FlowableProcessor<IndexingOperation> indexProcessor;

   public SearchIndexerImpl(PojoIndexer delegate, EntityConverter entityConverter,
         InfinispanTypeContextProvider typeContextProvider, BlockingManager blockingManager,
         NonBlockingManager nonBlockingManager) {
      this.delegate = delegate;
      this.entityConverter = entityConverter;
      this.typeContextProvider = typeContextProvider;
      this.nonBlockingManager = nonBlockingManager;

      this.indexProcessor = UnicastProcessor.<IndexingOperation>create().toSerialized();

      indexProcessor
            .observeOn(Schedulers.from(blockingManager.asExecutor("search-indexer")))
            .subscribe(operation -> operation.invoke(delegate, nonBlockingManager),
                  throwable -> log.errorProcessingIndexingOperation(throwable));
   }

   @Override
   public CompletableFuture<?> add(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity);
      if (convertedValue == null) {
         return CompletableFutures.completedNull();
      }

      AddIndexingOperation operation = new AddIndexingOperation(delegate, providedId, routingKey, convertedValue);
      indexProcessor.onNext(operation);
      return operation;
   }

   @Override
   public CompletableFuture<?> addOrUpdate(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity);
      if (convertedValue == null) {
         return CompletableFutures.completedNull();
      }

      AddOrUpdateIndexingOperation operation = new AddOrUpdateIndexingOperation(delegate, providedId, routingKey, convertedValue);
      indexProcessor.onNext(operation);
      return operation;
   }

   @Override
   public CompletableFuture<?> delete(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity);
      if (convertedValue == null) {
         return CompletableFutures.completedNull();
      }

      DeleteIndexingOperation operation = new DeleteIndexingOperation(delegate, providedId, routingKey, convertedValue);
      indexProcessor.onNext(operation);
      return operation;
   }

   @Override
   public CompletableFuture<?> purge(Object providedId, String routingKey) {
      PurgeIndexingOperation operation = new PurgeIndexingOperation(typeContextProvider, delegate, providedId, routingKey);
      indexProcessor.onNext(operation);
      return operation;
   }

   @Override
   public void close() {
      indexProcessor.onComplete();
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

   static class ConvertedValue {
      PojoRawTypeIdentifier<?> typeIdentifier;
      Object value;

      public ConvertedValue(InfinispanIndexedTypeContext<?> typeContext, Object value) {
         this.typeIdentifier = typeContext.typeIdentifier();
         this.value = value;
      }
   }
}
