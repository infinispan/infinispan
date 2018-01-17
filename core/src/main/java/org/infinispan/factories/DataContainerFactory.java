package org.infinispan.factories;

import org.infinispan.configuration.cache.EvictionConfiguration;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.container.DefaultDataContainer;
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

         long thresholdSize = configuration.memory().size();

         EvictionStrategy strategy = configuration.memory().evictionStrategy();
         //handle case when < 0 value signifies unbounded container or when we are not removal based
         if (strategy.isExceptionBased() || !strategy.isEnabled()) {
            if (configuration.memory().storageType() == StorageType.OFF_HEAP) {
               return (T) new OffHeapDataContainer(configuration.memory().addressCount());
            } else {
               return (T) DefaultDataContainer.unBoundedDataContainer(level);
            }
         }

         DataContainer dataContainer;
         if (configuration.memory().storageType() == StorageType.OFF_HEAP) {
            dataContainer = new BoundedOffHeapDataContainer(configuration.memory().addressCount(), thresholdSize,
                  configuration.memory().evictionType());
         } else {
            dataContainer = DefaultDataContainer.boundedDataContainer(level, thresholdSize,
                  configuration.memory().evictionType());
         }
         configuration.eviction().attributes().attribute(EvictionConfiguration.SIZE).addListener((newSize, old) ->
               configuration.memory().size(newSize.get()));
         configuration.memory().attributes().attribute(MemoryConfiguration.SIZE).addListener((newSize, old) ->
               dataContainer.resize(newSize.get()));
         return (T) dataContainer;
      }
   }
}
