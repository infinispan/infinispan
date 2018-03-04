package org.infinispan.factories;

import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.EvictionConfiguration;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.BoundedSegmentedDataContainer;
import org.infinispan.container.DataContainer;
import org.infinispan.container.DefaultDataContainer;
import org.infinispan.container.DefaultSegmentedDataContainer;
import org.infinispan.container.L1DefaultSegmentedDataContainer;
import org.infinispan.container.offheap.BoundedOffHeapDataContainer;
import org.infinispan.container.offheap.OffHeapDataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.factories.annotations.DefaultFactoryFor;

/**
 * Constructs the data container
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@DefaultFactoryFor(classes = DataContainer.class)
public class DataContainerFactory extends AbstractNamedCacheComponentFactory implements
         AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (configuration.dataContainer().dataContainer() != null) {
         return (T) configuration.dataContainer().dataContainer();
      } else {
         int level = configuration.locking().concurrencyLevel();

         MemoryConfiguration memoryConfiguration = configuration.memory();

         long thresholdSize = memoryConfiguration.size();

         EvictionStrategy strategy = memoryConfiguration.evictionStrategy();
         //handle case when < 0 value signifies unbounded container or when we are not removal based
         if (strategy.isExceptionBased() || !strategy.isEnabled()) {
            if (configuration.memory().storageType() == StorageType.OFF_HEAP) {
               return (T) new OffHeapDataContainer(memoryConfiguration.addressCount());
            } else {
               ClusteringConfiguration clusteringConfiguration = configuration.clustering();
               if (clusteringConfiguration.cacheMode().needsStateTransfer()) {
                  int segments = clusteringConfiguration.hash().numSegments();
                  if (clusteringConfiguration.l1().enabled()) {
                     return (T) new L1DefaultSegmentedDataContainer<>(segments);
                  }
                  return (T) new DefaultSegmentedDataContainer<>(segments);
               } else {
                  return (T) DefaultDataContainer.unBoundedDataContainer(level);
               }
            }
         }

         DataContainer dataContainer;
         if (memoryConfiguration.storageType() == StorageType.OFF_HEAP) {
            dataContainer = new BoundedOffHeapDataContainer(memoryConfiguration.addressCount(), thresholdSize,
                  memoryConfiguration.evictionType());
         } else {
            ClusteringConfiguration clusteringConfiguration = configuration.clustering();
            if (clusteringConfiguration.cacheMode().needsStateTransfer()) {
               int segments = clusteringConfiguration.hash().numSegments();
               dataContainer = new BoundedSegmentedDataContainer<>(segments, thresholdSize,
                     memoryConfiguration.evictionType());
            } else {
               dataContainer = DefaultDataContainer.boundedDataContainer(level, thresholdSize,
                     memoryConfiguration.evictionType());
            }
         }
         configuration.eviction().attributes().attribute(EvictionConfiguration.SIZE).addListener((newSize, old) ->
               memoryConfiguration.size(newSize.get()));
         memoryConfiguration.attributes().attribute(MemoryConfiguration.SIZE).addListener((newSize, old) ->
               dataContainer.resize(newSize.get()));
         return (T) dataContainer;
      }
   }
}
