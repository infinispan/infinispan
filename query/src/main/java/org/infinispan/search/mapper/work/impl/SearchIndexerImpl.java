package org.infinispan.search.mapper.work.impl;

import java.util.concurrent.CompletableFuture;

import org.hibernate.search.engine.backend.work.execution.DocumentCommitStrategy;
import org.hibernate.search.engine.backend.work.execution.DocumentRefreshStrategy;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.route.DocumentRoutesDescriptor;
import org.hibernate.search.mapper.pojo.work.spi.PojoIndexer;
import org.infinispan.search.mapper.mapping.EntityConverter;
import org.infinispan.search.mapper.session.impl.InfinispanIndexedTypeContext;
import org.infinispan.search.mapper.session.impl.InfinispanTypeContextProvider;
import org.infinispan.search.mapper.work.SearchIndexer;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.NonBlockingManager;

import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * @author Fabio Massimo Ercoli
 */
public class SearchIndexerImpl implements SearchIndexer, Consumer<SearchIndexerImpl.IndexerOperation> {

   private final PojoIndexer delegate;
   private final EntityConverter entityConverter;
   private final InfinispanTypeContextProvider typeContextProvider;
   private final NonBlockingManager nonBlockingManager;

   private final FlowableProcessor<IndexerOperation> indexProcessor;

   public SearchIndexerImpl(PojoIndexer delegate, EntityConverter entityConverter,
         InfinispanTypeContextProvider typeContextProvider, BlockingManager blockingManager,
         NonBlockingManager nonBlockingManager) {
      this.delegate = delegate;
      this.entityConverter = entityConverter;
      this.typeContextProvider = typeContextProvider;
      this.nonBlockingManager = nonBlockingManager;

      this.indexProcessor = UnicastProcessor.<IndexerOperation>create().toSerialized();

      indexProcessor
            .observeOn(Schedulers.from(blockingManager.asExecutor("search-indexer")))
            // TODO: log this error
            .subscribe(this, e -> e.printStackTrace());

   }

   abstract class IndexerOperation extends CompletableFuture<Void> {
      abstract void invoke(PojoIndexer pojoIndexer, NonBlockingManager nonBlockingManager);
   }

   class AddIndexerOperation extends IndexerOperation {
      private final Object providedId;
      private final String routingKey;
      private final ConvertedValue convertedValue;

      AddIndexerOperation(Object providedId, String routingKey, ConvertedValue convertedValue) {
         this.providedId = providedId;
         this.routingKey = routingKey;
         this.convertedValue = convertedValue;
      }

      @Override
      void invoke(PojoIndexer pojoIndexer, NonBlockingManager nonBlockingManager) {
         delegate.add(convertedValue.typeIdentifier, providedId,
                     DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
                     DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE)
               .whenComplete((v, t) -> {
                  if (t != null) {
                     nonBlockingManager.completeExceptionally(this, t);
                  } else {
                     nonBlockingManager.complete(this, null);
                  }
               });
      }
   }

   @Override
   public CompletableFuture<?> add(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity);
      if (convertedValue == null) {
         return CompletableFuture.completedFuture(null);
      }

      AddIndexerOperation addIndexerOperation = new AddIndexerOperation(providedId, routingKey, convertedValue);
      indexProcessor.onNext(addIndexerOperation);
      return addIndexerOperation;
   }

   @Override
   public CompletableFuture<?> addOrUpdate(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity);
      if (convertedValue == null) {
         return CompletableFuture.completedFuture(null);
      }

      // TODO: needs task added and use flowable
      return delegate.addOrUpdate(convertedValue.typeIdentifier, providedId,
            DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
            DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE);
   }

   @Override
   public CompletableFuture<?> delete(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity);
      if (convertedValue == null) {
         return CompletableFuture.completedFuture(null);
      }

      // TODO: needs task added and use flowable
      return delegate.delete(convertedValue.typeIdentifier, providedId,
            DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
            DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE);
   }

   @Override
   public CompletableFuture<?> purge(Object providedId, String routingKey) {
      // TODO: needs task added and use flowable
      return CompletableFuture.allOf(typeContextProvider.allTypeIdentifiers().stream()
            .map((typeIdentifier) -> delegate.delete(typeIdentifier, providedId,
                  DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey),
                  DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE)).toArray(CompletableFuture[]::new));
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

   @Override
   public void accept(IndexerOperation o) {
      o.invoke(delegate, nonBlockingManager);
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
