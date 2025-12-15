package org.infinispan.factories;

import java.util.function.Supplier;

import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.impl.BoundedSegmentedDataContainer;
import org.infinispan.container.impl.DefaultDataContainer;
import org.infinispan.container.impl.DefaultSegmentedDataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.L1SegmentedDataContainer;
import org.infinispan.container.impl.PeekableTouchableContainerMap;
import org.infinispan.container.impl.PeekableTouchableMap;
import org.infinispan.container.impl.SharedCaffeineMap;
import org.infinispan.container.impl.SharedContainerMaps;
import org.infinispan.container.offheap.BoundedOffHeapDataContainer;
import org.infinispan.container.offheap.OffHeapConcurrentMap;
import org.infinispan.container.offheap.OffHeapDataContainer;
import org.infinispan.container.offheap.OffHeapEntryFactory;
import org.infinispan.container.offheap.OffHeapMemoryAllocator;
import org.infinispan.container.offheap.SegmentedBoundedOffHeapDataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;

/**
 * Constructs the data container
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@DefaultFactoryFor(classes = InternalDataContainer.class)
public class DataContainerFactory extends AbstractNamedCacheComponentFactory implements
      AutoInstantiableFactory {
   @Inject SharedContainerMaps sharedContainerMaps;

   @Override
   public Object construct(String componentName) {
      ClusteringConfiguration clusteringConfiguration = configuration.clustering();

      boolean shouldSegment = Configurations.needSegments(configuration);
      int level = configuration.locking().concurrencyLevel();

      MemoryConfiguration memoryConfiguration = configuration.memory();
      boolean offHeap = memoryConfiguration.isOffHeap();

      String containerMapName = memoryConfiguration.evictionContainer();
      int segments = shouldSegment ? clusteringConfiguration.hash().numSegments() : 1;
      if (containerMapName != null) {
         String cacheName = componentRegistry.getCacheName();
         SharedCaffeineMap<?, ?> sharedCaffeineMap = sharedContainerMaps.getMap(containerMapName);
         if (sharedCaffeineMap == null) {
            throw new IllegalStateException("Shared container not available: " + containerMapName);
         }
         return sharedCaffeineMap.newContainer(cacheName, basicComponentRegistry, segments);
      }

      EvictionStrategy strategy = memoryConfiguration.whenFull();
      //handle case when < 0 value signifies unbounded container or when we are not removal based
      if (strategy.isExceptionBased() || !strategy.isEnabled()) {
         if (offHeap) {
            if (shouldSegment) {
               Supplier<PeekableTouchableMap<WrappedBytes, WrappedBytes>> mapSupplier =
                     this::createAndStartOffHeapConcurrentMap;
               if (clusteringConfiguration.l1().enabled()) {
                  return new L1SegmentedDataContainer<>(mapSupplier, segments);
               }
               return new DefaultSegmentedDataContainer<>(mapSupplier, segments);
            } else {
               return new OffHeapDataContainer();
            }
         } else if (shouldSegment) {
            Supplier<PeekableTouchableMap<Object, Object>> mapSupplier =
                  PeekableTouchableContainerMap::new;
            if (clusteringConfiguration.l1().enabled()) {
               return new L1SegmentedDataContainer<>(mapSupplier, segments);
            }
            return new DefaultSegmentedDataContainer<>(mapSupplier, segments);
         } else {
            return DefaultDataContainer.unBoundedDataContainer(level);
         }
      }

      boolean sizeInBytes = memoryConfiguration.maxSize() != null;
      long thresholdSize = sizeInBytes ? memoryConfiguration.maxSizeBytes() : memoryConfiguration.maxCount();

      DataContainer<?, ?> dataContainer;
      if (offHeap) {
         if (shouldSegment) {
            dataContainer = new SegmentedBoundedOffHeapDataContainer(segments, thresholdSize, sizeInBytes);
         } else {
            dataContainer = new BoundedOffHeapDataContainer(thresholdSize, sizeInBytes);
         }
      } else if (shouldSegment) {
         dataContainer = new BoundedSegmentedDataContainer<>(segments, thresholdSize, sizeInBytes);
      } else {
         dataContainer = DefaultDataContainer.boundedDataContainer(level, thresholdSize, sizeInBytes);
      }
      if (sizeInBytes) {
         memoryConfiguration.attributes().attribute(MemoryConfiguration.MAX_SIZE)
                            .addListener((newSize, old) -> dataContainer.resize(memoryConfiguration.maxSizeBytes()));
      } else {
         memoryConfiguration.attributes().attribute(MemoryConfiguration.MAX_COUNT)
                            .addListener((newSize, old) -> dataContainer.resize(newSize.get()));
      }
      return dataContainer;
   }

   /* visible for testing */
   OffHeapConcurrentMap createAndStartOffHeapConcurrentMap() {
      OffHeapEntryFactory entryFactory = componentRegistry.getOrCreateComponent(OffHeapEntryFactory.class);
      OffHeapMemoryAllocator memoryAllocator = componentRegistry.getOrCreateComponent(OffHeapMemoryAllocator.class);
      return new OffHeapConcurrentMap(memoryAllocator, entryFactory, null);
   }
}
