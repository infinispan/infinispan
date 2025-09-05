package org.infinispan.query.mapper.work.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import org.hibernate.search.engine.backend.work.execution.DocumentCommitStrategy;
import org.hibernate.search.engine.backend.work.execution.DocumentRefreshStrategy;
import org.hibernate.search.engine.backend.work.execution.OperationSubmitter;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.route.DocumentRoutesDescriptor;
import org.hibernate.search.mapper.pojo.work.spi.PojoIndexer;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.core.impl.Log;
import org.infinispan.query.impl.IndexerConfig;
import org.infinispan.query.mapper.mapping.EntityConverter;
import org.infinispan.query.mapper.session.impl.InfinispanIndexedTypeContext;
import org.infinispan.query.mapper.session.impl.InfinispanTypeContextProvider;
import org.infinispan.query.mapper.work.SearchIndexer;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.internal.functions.Functions;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;

/**
 * @author Fabio Massimo Ercoli
 */
public class SearchIndexerImpl implements SearchIndexer {
   private static final Log log = LogFactory.getLog(QueryInterceptor.class, Log.class);

   private final PojoIndexer delegate;
   private final EntityConverter entityConverter;
   private final InfinispanTypeContextProvider typeContextProvider;
   private final BlockingManager blockingManager;
   private final IndexerConfig indexerConfig;
   private final ConcurrentMap<Supplier<Flowable<?>>, CompletableFuture<?>> submittedTasks;

   private Disposable processorDisposer;

   private FlowableProcessor<Supplier<Flowable<?>>> requestProcessor;

   public SearchIndexerImpl(PojoIndexer delegate, EntityConverter entityConverter,
                            InfinispanTypeContextProvider typeContextProvider, BlockingManager blockingManager,
                            IndexerConfig indexerConfig) {
      this.delegate = delegate;
      this.entityConverter = entityConverter;
      this.typeContextProvider = typeContextProvider;
      this.blockingManager = blockingManager;
      this.indexerConfig = indexerConfig;

      this.submittedTasks = new ConcurrentHashMap<>();
   }

   @Override
   public void start() {
      requestProcessor = UnicastProcessor.<Supplier<Flowable<?>>>create().toSerialized();
      // onBackpressureDrop will drop any item that it can't immediately pass downstream
      // Thus next downstream must batch them to not immediately drop
      processorDisposer = requestProcessor.onBackpressureDrop(supplier -> {
                     CompletableFuture<?> completableFuture = submittedTasks.remove(supplier);
                     if (completableFuture == null) {
                        throw new IllegalStateException("Dropped task " + supplier + " not found in submittedTask " + submittedTasks);
                     }
                     completableFuture.completeExceptionally(log.hibernateSearchBackpressure());
                  })
            // This will only request up to maxConcurrency items at the same time
            .flatMap(Supplier::get, indexerConfig.maxConcurrency())
            // Clear the submittedTasks on error/completion/cancel just in case
            .doFinally(submittedTasks::clear)
            .subscribe(Functions.emptyConsumer(),
                  t -> log.fatal("Indexing encountered a non recoverable error", t));
   }

   private CompletableFuture<?> sendOperation(Supplier<CompletionStage<?>> actualSupplier) {
      CompletableFuture<Object> future = new CompletableFuture<>();

      Supplier<Flowable<?>> supplier = () -> {
         try {
            actualSupplier.get()
                  .whenCompleteAsync((v, t) -> {
                     if (t != null) {
                        future.completeExceptionally(t);
                     } else {
                        future.complete(v);
                     }
                  }, blockingManager.nonBlockingExecutor());
         } catch (Throwable t) {
            blockingManager.nonBlockingExecutor().execute(() -> future.completeExceptionally(t));
         }
         return RxJavaInterop.voidCompletionStageToFlowable(future
               // This ignores any exception for the processor. The original exception is propagated
               // to the returned future so the user can receive
               .exceptionally(CompletableFutures.toNullFunction()));
      };

      submittedTasks.put(supplier, future);
      // Make sure to clean up the submittedTasks when the future is complete as it could be dropped after onNext has
      // been completed
      future.whenComplete((v, t) -> submittedTasks.remove(supplier));

      requestProcessor.onNext(supplier);
      return future;
   }

   @Override
   public CompletableFuture<?> add(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity, providedId);
      if (convertedValue == null) {
         return CompletableFutures.completedNull();
      }

      return sendOperation(() ->
            delegate.add(convertedValue.typeIdentifier, providedId,
               DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
               DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
               OperationSubmitter.rejecting()));
   }

   @Override
   public CompletableFuture<?> addOrUpdate(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity, providedId);
      if (convertedValue == null) {
         return CompletableFutures.completedNull();
      }

      return sendOperation(() ->
            delegate.addOrUpdate(convertedValue.typeIdentifier, providedId,
                  DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
                  DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
                  OperationSubmitter.rejecting()));
   }

   @Override
   public CompletableFuture<?> delete(Object providedId, String routingKey, Object entity) {
      ConvertedValue convertedValue = convertedValue(entity, providedId);
      if (convertedValue == null) {
         return CompletableFutures.completedNull();
      }

      return sendOperation(() ->
            delegate.delete(convertedValue.typeIdentifier, providedId,
                  DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
                  DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
                  OperationSubmitter.rejecting()));
   }

   @Override
   public CompletableFuture<?> purge(Object providedId, String routingKey) {
      return CompletableFuture.allOf(typeContextProvider.allTypeIdentifiers().stream()
            .map((typeIdentifier) -> sendOperation(() -> delegate.delete(typeIdentifier, providedId,
                  DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey),
                  DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE,
                  OperationSubmitter.rejecting())))
            .toArray(CompletableFuture[]::new));
   }

   @Override
   public void close() {
      processorDisposer.dispose();
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
