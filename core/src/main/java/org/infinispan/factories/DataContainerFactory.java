package org.infinispan.factories;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.EntrySizeCalculator;
import org.infinispan.configuration.cache.EvictionConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.DefaultDataContainer;
import org.infinispan.container.entries.PrimitiveEntrySizeCalculator;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.marshall.core.WrappedByteArraySizeCalculator;

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
         EvictionStrategy st = configuration.eviction().strategy();
         int level = configuration.locking().concurrencyLevel();
         Equivalence keyEquivalence = configuration.dataContainer().keyEquivalence();

         long thresholdSize = configuration.eviction().size();


         //handle case when < 0 value signifies unbounded container
         if(thresholdSize < 0) {
            return (T) DefaultDataContainer.unBoundedDataContainer(level);
         }

         DefaultDataContainer dataContainer;
         switch (st) {
            case NONE:
               return (T) DefaultDataContainer.unBoundedDataContainer(level);
            case UNORDERED:
            case LRU:

               if (configuration.eviction().type() == EvictionType.MEMORY) {
                  EntrySizeCalculator esc = new WrappedByteArraySizeCalculator<>(new PrimitiveEntrySizeCalculator());
                  dataContainer = DefaultDataContainer.boundedDataContainer(
                          level, thresholdSize, st, configuration.eviction().threadPolicy(), esc);
                  break;
               }
            case FIFO:
            case LIRS:
               dataContainer = DefaultDataContainer.boundedDataContainer(
                  level, thresholdSize, st, configuration.eviction().threadPolicy(), configuration.eviction().type());
               break;
            default:
               throw new CacheConfigurationException("Unknown eviction strategy "
                        + configuration.eviction().strategy());
         }
         configuration.eviction().attributes().attribute(EvictionConfiguration.SIZE).addListener((newSize, old) -> {
            dataContainer.resize(newSize.get());
         });
         return (T) dataContainer;
      }
   }
}
