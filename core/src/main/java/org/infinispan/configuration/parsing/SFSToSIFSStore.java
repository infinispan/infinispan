package org.infinispan.configuration.parsing;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.internal.PersistenceUtil;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.support.DelegatingInitializationContext;
import org.infinispan.persistence.support.DelegatingNonBlockingStore;
import org.infinispan.persistence.support.SegmentPublisherWrapper;
import org.infinispan.util.concurrent.CompletionStages;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.flowables.GroupedFlowable;

/**
 * Store that is used to migrate data from ISPN 12.0 SingleFileStore to an ISPN 13.0 SoftIndexFileStore.
 * This store works identically to a SoftIndexFileStore except that it will first attempt to copy all the entries
 * from a SingleFileStore with the same location as the configured {@link SoftIndexFileStoreConfiguration#dataLocation()}.
 * Note that both a segmented and non segmented SingleFileStore is attempted since it could have been either and
 * SoftIndexFileStore only supports segmentation now.
 * @param <K> key type
 * @param <V> value type
 */
@ConfiguredBy(SFSToSIFSConfiguration.class)
public class SFSToSIFSStore<K, V> extends DelegatingNonBlockingStore<K, V> {
   private NonBlockingStore<K, V> targetStore;

   @Override
   public NonBlockingStore<K, V> delegate() {
      return targetStore;
   }

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      SFSToSIFSConfiguration sfsToSIFSConfiguration = ctx.getConfiguration();

      CompletionStage<NonBlockingStore<K, V>> targetStage = createAndStartSoftIndexFileStore(ctx, sfsToSIFSConfiguration);

      // If config has purge don't even bother with migration
      if (sfsToSIFSConfiguration.purgeOnStartup()) {
         return targetStage.thenAccept(sifsStore -> targetStore = sifsStore);
      }

      CompletionStage<NonBlockingStore<K, V>> nonSegmentedSFSStage = createAndStartSingleFileStore(ctx, sfsToSIFSConfiguration.dataLocation(), false);
      CompletionStage<NonBlockingStore<K, V>> segmentedSFSStage = createAndStartSingleFileStore(ctx, sfsToSIFSConfiguration.dataLocation(), true);

      Function<NonBlockingStore<K, V>, CompletionStage<Void>> composed = sourceStore -> targetStage.thenCompose(targetStore -> {
         this.targetStore = targetStore;

         int segmentCount = ctx.getCache().getCacheConfiguration().clustering().hash().numSegments();
         IntSet allSegments = IntSets.immutableRangeSet(segmentCount);

         Flowable<GroupedFlowable<Integer, MarshallableEntry<K, V>>> groupedFlowable =
               Flowable.fromPublisher(sourceStore.publishEntries(allSegments, null, true))
                     .groupBy(ctx.getKeyPartitioner()::getSegment);

         return targetStore.batch(segmentCount, Flowable.empty(), groupedFlowable.map(SegmentPublisherWrapper::wrap))
               .thenCompose(ignore -> sourceStore.destroy());
      });

      return CompletionStages.allOf(nonSegmentedSFSStage.thenCompose(composed),
            segmentedSFSStage.thenCompose(composed));
   }

   CompletionStage<NonBlockingStore<K, V>> createAndStartSingleFileStore(InitializationContext ctx, String location,
         boolean segmented) {
      AttributeSet sfsAttributes = SingleFileStoreConfiguration.attributeDefinitionSet();
      sfsAttributes.attribute(SingleFileStoreConfiguration.LOCATION).set(location);
      sfsAttributes.attribute(AbstractStoreConfiguration.SEGMENTED).set(segmented);
      sfsAttributes.attribute(AbstractStoreConfiguration.READ_ONLY).set(Boolean.TRUE);

      return createAndStartStore(ctx, new SingleFileStoreConfiguration(sfsAttributes.protect(),
            new AsyncStoreConfiguration(AsyncStoreConfiguration.attributeDefinitionSet().protect())));
   }

   CompletionStage<NonBlockingStore<K, V>> createAndStartSoftIndexFileStore(InitializationContext ctx,
         SFSToSIFSConfiguration config) {
      return createAndStartStore(ctx, new SoftIndexFileStoreConfiguration(config.attributes(),
            config.async(), config.index(), config.data()));
   }

   private CompletionStage<NonBlockingStore<K, V>> createAndStartStore(InitializationContext ctx, StoreConfiguration storeConfiguration) {
      NonBlockingStore<K, V> store = PersistenceUtil.storeFromConfiguration(storeConfiguration);
      return store.start(new DelegatingInitializationContext() {
         @Override
         public InitializationContext delegate() {
            return ctx;
         }

         @SuppressWarnings("unchecked")
         @Override
         public <T extends StoreConfiguration> T getConfiguration() {
            return (T) storeConfiguration;
         }
      }).thenApply(ignore -> store);
   }
}
