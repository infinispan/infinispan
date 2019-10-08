package org.infinispan.factories;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.MemoryStorageConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.container.impl.BoundedSegmentedDataContainer;
import org.infinispan.container.impl.DefaultDataContainer;
import org.infinispan.container.impl.DefaultSegmentedDataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.L1SegmentedDataContainer;
import org.infinispan.container.offheap.BoundedOffHeapDataContainer;
import org.infinispan.container.offheap.OffHeapConcurrentMap;
import org.infinispan.container.offheap.OffHeapDataContainer;
import org.infinispan.container.offheap.OffHeapEntryFactory;
import org.infinispan.container.offheap.OffHeapMemoryAllocator;
import org.infinispan.container.offheap.SegmentedBoundedOffHeapDataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.factories.annotations.DefaultFactoryFor;

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

   @Override
   @SuppressWarnings("unchecked")
   public Object construct(String componentName) {
      ClusteringConfiguration clusteringConfiguration = configuration.clustering();

      boolean shouldSegment = clusteringConfiguration.cacheMode().needsStateTransfer();
      int level = configuration.locking().concurrencyLevel();

      MemoryConfiguration memoryConfiguration = configuration.memory();

      EvictionStrategy strategy = memoryConfiguration.evictionStrategy();
      //handle case when < 0 value signifies unbounded container or when we are not removal based
      if (strategy.isExceptionBased() || !strategy.isEnabled()) {
         if (configuration.memory().storageType() == StorageType.OFF_HEAP) {
            if (shouldSegment) {
               int segments = clusteringConfiguration.hash().numSegments();
               Supplier mapSupplier = this::createAndStartOffHeapConcurrentMap;
               if (clusteringConfiguration.l1().enabled()) {
                  return new L1SegmentedDataContainer<>(mapSupplier, segments);
               }
               return new DefaultSegmentedDataContainer<>(mapSupplier, segments);
            } else {
               return new OffHeapDataContainer();
            }
         } else if (shouldSegment) {
            Supplier mapSupplier = ConcurrentHashMap::new;
            int segments = clusteringConfiguration.hash().numSegments();
            if (clusteringConfiguration.l1().enabled()) {
               return new L1SegmentedDataContainer<>(mapSupplier, segments);
            }
            return new DefaultSegmentedDataContainer<>(mapSupplier, segments);
         } else {
            return DefaultDataContainer.unBoundedDataContainer(level);
         }
      }

      long thresholdSize = memoryConfiguration.size();

      DataContainer dataContainer;
      if (memoryConfiguration.storageType() == StorageType.OFF_HEAP) {
         if (shouldSegment) {
            int segments = clusteringConfiguration.hash().numSegments();
            dataContainer = new SegmentedBoundedOffHeapDataContainer(segments, thresholdSize,
                  memoryConfiguration.evictionType());
         } else {
            dataContainer = new BoundedOffHeapDataContainer(thresholdSize, memoryConfiguration.evictionType());
         }
      } else if (shouldSegment) {
         int segments = clusteringConfiguration.hash().numSegments();
         dataContainer = new BoundedSegmentedDataContainer<>(segments, thresholdSize,
               memoryConfiguration.evictionType());
      } else {
         dataContainer = DefaultDataContainer.boundedDataContainer(level, thresholdSize,
               memoryConfiguration.evictionType());
      }
      memoryConfiguration.heapConfiguration().attributes().attribute(MemoryStorageConfiguration.SIZE).addListener((newSize, old) ->
            dataContainer.resize(newSize.get()));
      return dataContainer;
   }

   /* visible for testing */
   OffHeapConcurrentMap createAndStartOffHeapConcurrentMap() {
      OffHeapEntryFactory entryFactory = componentRegistry.getOrCreateComponent(OffHeapEntryFactory.class);
      OffHeapMemoryAllocator memoryAllocator = componentRegistry.getOrCreateComponent(OffHeapMemoryAllocator.class);
      return new OffHeapConcurrentMap(memoryAllocator, entryFactory, null);
   }
}
