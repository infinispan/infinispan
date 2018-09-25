package org.infinispan.factories;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.EvictionConfiguration;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.container.impl.BoundedSegmentedDataContainer;
import org.infinispan.container.impl.DefaultDataContainer;
import org.infinispan.container.impl.DefaultSegmentedDataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalDataContainerAdapter;
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

   public static final String SEGMENTATION_FEATURE = "data-segmentation";

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      DataContainer customDataContainer = configuration.dataContainer().dataContainer();
      if (customDataContainer != null) {
         if (customDataContainer instanceof InternalDataContainer) {
            return (T) customDataContainer;
         }
         return (T) new InternalDataContainerAdapter<>(customDataContainer);
      } else {
         ClusteringConfiguration clusteringConfiguration = configuration.clustering();

         boolean shouldSegment = globalConfiguration.features().isAvailable(SEGMENTATION_FEATURE) &&
               clusteringConfiguration.cacheMode().needsStateTransfer();
         int level = configuration.locking().concurrencyLevel();

         MemoryConfiguration memoryConfiguration = configuration.memory();

         EvictionStrategy strategy = memoryConfiguration.evictionStrategy();
         //handle case when < 0 value signifies unbounded container or when we are not removal based
         if (strategy.isExceptionBased() || !strategy.isEnabled()) {
            if (configuration.memory().storageType() == StorageType.OFF_HEAP) {

               int addressCount = memoryConfiguration.addressCount();
               if (shouldSegment) {
                  int segments = clusteringConfiguration.hash().numSegments();
                  Supplier mapSupplier = () -> {
                     OffHeapEntryFactory entryFactory = componentRegistry.getOrCreateComponent(OffHeapEntryFactory.class);
                     OffHeapMemoryAllocator memoryAllocator = componentRegistry.getOrCreateComponent(OffHeapMemoryAllocator.class);
                     // TODO: find better way to handle size here or is it okay? internally it will round to next power of 2
                     OffHeapConcurrentMap offHeapMap = new OffHeapConcurrentMap(addressCount / segments, memoryAllocator, entryFactory, null);
                     offHeapMap.start();
                     return offHeapMap;
                  };
                  if (clusteringConfiguration.l1().enabled()) {
                     return (T) new L1SegmentedDataContainer<>(mapSupplier, segments);
                  }
                  return (T) new DefaultSegmentedDataContainer<>(mapSupplier, segments);
               } else {
                  return (T) new OffHeapDataContainer(addressCount);
               }
            } else if (shouldSegment) {
               Supplier mapSupplier = ConcurrentHashMap::new;
               int segments = clusteringConfiguration.hash().numSegments();
               if (clusteringConfiguration.l1().enabled()) {
                  return (T) new L1SegmentedDataContainer<>(mapSupplier, segments);
               }
               return (T) new DefaultSegmentedDataContainer<>(mapSupplier, segments);
            } else {
               return (T) DefaultDataContainer.unBoundedDataContainer(level);
            }
         }

         long thresholdSize = memoryConfiguration.size();

         DataContainer dataContainer;
         if (memoryConfiguration.storageType() == StorageType.OFF_HEAP) {
            int addressCount = memoryConfiguration.addressCount();
            if (shouldSegment) {
               int segments = clusteringConfiguration.hash().numSegments();
               dataContainer = new SegmentedBoundedOffHeapDataContainer(addressCount, segments, thresholdSize,
                     memoryConfiguration.evictionType());
            } else {
               dataContainer = new BoundedOffHeapDataContainer(addressCount, thresholdSize,
                     memoryConfiguration.evictionType());
            }
         } else if (shouldSegment) {
            int segments = clusteringConfiguration.hash().numSegments();
            dataContainer = new BoundedSegmentedDataContainer<>(segments, thresholdSize,
                  memoryConfiguration.evictionType());
         } else {
            dataContainer = DefaultDataContainer.boundedDataContainer(level, thresholdSize,
                  memoryConfiguration.evictionType());
         }
         configuration.eviction().attributes().attribute(EvictionConfiguration.SIZE).addListener((newSize, old) ->
               memoryConfiguration.size(newSize.get()));
         memoryConfiguration.attributes().attribute(MemoryConfiguration.SIZE).addListener((newSize, old) ->
               dataContainer.resize(newSize.get()));
         return (T) dataContainer;
      }
   }
}
