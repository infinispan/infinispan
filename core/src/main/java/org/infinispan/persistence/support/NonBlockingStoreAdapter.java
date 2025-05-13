package org.infinispan.persistence.support;

import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.persistence.spi.AdvancedCacheExpirationWriter;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.ExternalStore;
import org.infinispan.persistence.spi.FlagAffectedStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;
import org.infinispan.persistence.spi.TransactionalCacheWriter;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import jakarta.transaction.Transaction;

public class NonBlockingStoreAdapter<K, V> implements NonBlockingStore<K, V> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final AtomicInteger id = new AtomicInteger();

   private final Lifecycle oldStoreImpl;
   private final Set<Characteristic> characteristics;

   private BlockingManager blockingManager;
   private MarshallableEntryFactory<K, V> marshallableEntryFactory;

   public NonBlockingStoreAdapter(Lifecycle oldStoreImpl) {
      this.oldStoreImpl = oldStoreImpl;
      this.characteristics = determineCharacteristics(oldStoreImpl);
   }

   public Lifecycle getActualStore() {
      return oldStoreImpl;
   }

   private String nextTraceId(String operationName) {
      return log.isTraceEnabled() ? "StoreAdapter-" + operationName + "-" + id.getAndIncrement() : null;
   }

   private static Set<Characteristic> determineCharacteristics(Object storeImpl) {
      EnumSet<Characteristic> characteristics;
      if (storeImpl instanceof SegmentedAdvancedLoadWriteStore) {
          characteristics = EnumSet.of(Characteristic.SEGMENTABLE, Characteristic.EXPIRATION,
               Characteristic.BULK_READ);
      } else {
         characteristics = EnumSet.noneOf(Characteristic.class);
         if (storeImpl instanceof AdvancedCacheLoader) {
            characteristics.add(Characteristic.BULK_READ);
         } else if (!(storeImpl instanceof CacheLoader)) {
            characteristics.add(Characteristic.WRITE_ONLY);
         }

         if (storeImpl instanceof AdvancedCacheWriter) {
            characteristics.add(Characteristic.EXPIRATION);
         } else if (!(storeImpl instanceof CacheWriter)) {
            characteristics.add(Characteristic.READ_ONLY);
         }
      }

      Store storeAnnotation = storeImpl.getClass().getAnnotation(Store.class);
      if (storeAnnotation != null && storeAnnotation.shared()) {
         characteristics.add(Characteristic.SHAREABLE);
      }

      // Transactional is a special interface that could be true on a segment or not segmented store both
      if (storeImpl instanceof TransactionalCacheWriter) {
         characteristics.add(Characteristic.TRANSACTIONAL);
      }
      return characteristics;
   }

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      blockingManager = ctx.getBlockingManager();
      marshallableEntryFactory = ctx.getMarshallableEntryFactory();
      return blockingManager.runBlocking(() -> {
         if (isReadOnly()) {
            loader().init(ctx);
         } else {
            writer().init(ctx);
         }
         oldStoreImpl.start();
      }, nextTraceId("start"));
   }

   @Override
   public CompletionStage<Void> stop() {
      return blockingManager.runBlocking(oldStoreImpl::stop, nextTraceId("stop"));
   }

   @Override
   public CompletionStage<Void> destroy() {
      return blockingManager.runBlocking(() -> {
         if (oldStoreImpl instanceof ExternalStore) {
            ((ExternalStore<?, ?>) oldStoreImpl).destroy();
         } else {
            oldStoreImpl.stop();
         }
      }, nextTraceId("destroy"));
   }

   @Override
   public Set<Characteristic> characteristics() {
      return characteristics;
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      return blockingManager.supplyBlocking(() ->
            isSegmented() ? segmentedStore().size(segments) : advancedLoader().size(), nextTraceId("size"))
            .thenApply(Integer::longValue);
   }

   @Override
   public CompletionStage<Long> approximateSize(IntSet segments) {
      // Old SPI didn't support approximations
      return SIZE_UNAVAILABLE_FUTURE;
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean includeValues) {
      Publisher<MarshallableEntry<K, V>> publisher;
      if (isSegmented()) {
         publisher = segmentedStore().entryPublisher(segments, filter, includeValues, true);
      } else {
         publisher = advancedLoader().entryPublisher(filter, includeValues, true);
      }
      // Despite this being a publisher, we assume the subscription is blocking as the SPI never enforced this
      // We do however assume the creation of the Publisher is not blocking... maybe we should?
      return blockingManager.blockingPublisher(publisher);
   }

   @Override
   public Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      Publisher<K> publisher;
      if (isSegmented()) {
         publisher = segmentedStore().publishKeys(segments, filter);
      } else {
         publisher = advancedLoader().publishKeys(filter);
      }
      // Despite this being a publisher, we assume the subscription is blocking as the SPI never enforced this
      // We do however assume the creation of the Publisher is not blocking... maybe we should?
      return blockingManager.blockingPublisher(publisher);
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> purgeExpired() {
      return Flowable.defer(() -> {
         FlowableProcessor<MarshallableEntry<K, V>> flowableProcessor = UnicastProcessor.create();
         AdvancedCacheExpirationWriter.ExpirationPurgeListener<K, V> expirationPurgeListener = new AdvancedCacheExpirationWriter.ExpirationPurgeListener<K, V>() {
            @Override
            public void marshalledEntryPurged(MarshallableEntry<K, V> entry) {
               flowableProcessor.onNext(entry);
            }

            @Override
            public void entryPurged(K key) {
               flowableProcessor.onNext(marshallableEntryFactory.create(key));
            }
         };
         CompletionStage<Void> purgeStage;
         AdvancedCacheWriter<K, V> advancedCacheWriter = advancedWriter();
         if (advancedCacheWriter instanceof AdvancedCacheExpirationWriter) {
            purgeStage = blockingManager.runBlocking(() -> ((AdvancedCacheExpirationWriter<K, V>) advancedCacheWriter)
                  .purge(Runnable::run, expirationPurgeListener), nextTraceId("purgeExpired"));
         } else {
            purgeStage = blockingManager.runBlocking(() -> advancedCacheWriter
                  .purge(Runnable::run, expirationPurgeListener), nextTraceId("purgeExpired"));
         }
         purgeStage.whenComplete((ignore, t) -> {
            if (t != null) {
               flowableProcessor.onError(t);
            } else {
               flowableProcessor.onComplete();
            }
         });
         return flowableProcessor;
      });
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      return blockingManager.supplyBlocking(() ->
            isReadOnly() ? loader().isAvailable() : writer().isAvailable(), nextTraceId("isAvailable"));
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      return blockingManager.supplyBlocking(() ->
            isSegmented() ? segmentedStore().get(segment, key) : loader().loadEntry(key), nextTraceId("load"));
   }

   @Override
   public CompletionStage<Boolean> containsKey(int segment, Object key) {
      return blockingManager.supplyBlocking(() ->
            isSegmented() ? segmentedStore().contains(segment, key) : loader().contains(key), nextTraceId("containsKey"));
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      return blockingManager.runBlocking(() -> {
            if (isSegmented()) {
               segmentedStore().write(segment, entry);
            } else {
               writer().write(entry);
            }
      }, nextTraceId("write"));
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      return blockingManager.supplyBlocking(() ->
            isSegmented() ? segmentedStore().delete(segment, key) : writer().delete(key), nextTraceId("delete"));
   }

   @Override
   public CompletionStage<Void> addSegments(IntSet segments) {
      return blockingManager.runBlocking(() -> segmentedStore().addSegments(segments), nextTraceId("addSegments"));
   }

   @Override
   public CompletionStage<Void> removeSegments(IntSet segments) {
      return blockingManager.runBlocking(() -> segmentedStore().removeSegments(segments), nextTraceId("removeSegments"));
   }

   @Override
   public CompletionStage<Void> clear() {
      // Technically clear is defined on AdvancedCacheWriter - but there is no equivalent characteristic for that
      // so we have to double check the implementation
      if (oldStoreImpl instanceof AdvancedCacheWriter) {
         return blockingManager.runBlocking(advancedWriter()::clear, nextTraceId("clear"));
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> batch(int publisherCount, Publisher<NonBlockingStore.SegmentedPublisher<Object>> removePublisher,
         Publisher<NonBlockingStore.SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) {
      Flowable<Object> objectFlowable = Flowable.fromPublisher(removePublisher)
            .flatMap(RxJavaInterop.identityFunction(), false, publisherCount);
      Flowable<MarshallableEntry<? extends K, ? extends V>> meFlowable = Flowable.fromPublisher(writePublisher)
            .flatMap(RxJavaInterop.identityFunction(), false, publisherCount);

      return blockingManager.supplyBlocking(() -> {
         Single<Set<Object>> objectSingle = objectFlowable.collect(Collectors.toSet());
         objectSingle.subscribe(writer()::deleteBatch);
         // While bulkUpdate appears to be non blocking - there was no mandate that the operation actually be so.
         // Thus we run it on a blocking thread just in case
         return writer().bulkUpdate(meFlowable);
      }, nextTraceId("batch-update"))
            .thenCompose(Function.identity());
   }

   @Override
   public CompletionStage<Void> prepareWithModifications(Transaction transaction, int publisherCount,
         Publisher<SegmentedPublisher<Object>> removePublisher, Publisher<SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) {
      Set<Object> affectedKeys = new HashSet<>();
      BatchModification oldBatchModification = new BatchModification(affectedKeys);

      Flowable.fromPublisher(removePublisher)
            .subscribe(sp ->
               Flowable.fromPublisher(sp)
                     .subscribe(key -> {
                        affectedKeys.add(key);
                        oldBatchModification.removeEntry(key);
                     })
            );

      Flowable.fromPublisher(writePublisher)
            .subscribe(sp ->
                  Flowable.fromPublisher(sp)
                        .subscribe(me -> {
                           Object key = me.getKey();
                           affectedKeys.add(key);
                           //noinspection unchecked
                           oldBatchModification.addMarshalledEntry(key, (MarshallableEntry<Object, Object>) me);
                        })
            );

      return blockingManager.runBlocking(
            () -> transactionalStore().prepareWithModifications(transaction, oldBatchModification), nextTraceId("prepareWithModifications"));
   }

   @Override
   public CompletionStage<Void> commit(Transaction transaction) {
      return blockingManager.runBlocking(
            () -> transactionalStore().commit(transaction), nextTraceId("commit"));
   }

   @Override
   public CompletionStage<Void> rollback(Transaction transaction) {
      return blockingManager.runBlocking(
            () -> transactionalStore().rollback(transaction), nextTraceId("rollback"));
   }

   @Override
   public boolean ignoreCommandWithFlags(long commandFlags) {
      if (oldStoreImpl instanceof FlagAffectedStore) {
         return !((FlagAffectedStore) oldStoreImpl).shouldWrite(commandFlags);
      }
      return false;
   }

   boolean isSegmented() {
      return characteristics.contains(Characteristic.SEGMENTABLE);
   }

   boolean isReadOnly() {
      return characteristics.contains(Characteristic.READ_ONLY);
   }

   public TransactionalCacheWriter<K, V> transactionalStore() {
      return (TransactionalCacheWriter<K, V>) oldStoreImpl;
   }

   public SegmentedAdvancedLoadWriteStore<K, V> segmentedStore() {
      return (SegmentedAdvancedLoadWriteStore<K, V>) oldStoreImpl;
   }

   public AdvancedCacheLoader<K, V> advancedLoader() {
      return (AdvancedCacheLoader<K, V>) oldStoreImpl;
   }

   public AdvancedCacheWriter<K, V> advancedWriter() {
      return (AdvancedCacheWriter<K, V>) oldStoreImpl;
   }

   public CacheLoader<K, V> loader() {
      return (CacheLoader<K, V>) oldStoreImpl;
   }

   public CacheWriter<K, V> writer() {
      return (CacheWriter<K, V>) oldStoreImpl;
   }
}
