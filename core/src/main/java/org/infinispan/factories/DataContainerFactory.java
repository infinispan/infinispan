package org.infinispan.factories;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.concurrent.jdk8backported.EntrySizeCalculator;
import org.infinispan.container.DataContainer;
import org.infinispan.container.DefaultDataContainer;
import org.infinispan.container.entries.MarshalledValueEntrySizeCalculator;
import org.infinispan.container.entries.PrimitiveEntrySizeCalculator;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.eviction.EvictionType;
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
         EvictionStrategy st = configuration.eviction().strategy();
         int level = configuration.locking().concurrencyLevel();
         Equivalence keyEquivalence = configuration.dataContainer().keyEquivalence();

         long thresholdSize = configuration.eviction().size();

         //handle case when < 0 value signifies unbounded container
         if(thresholdSize < 0) {
            return (T) DefaultDataContainer.unBoundedDataContainer(
                    level, keyEquivalence);
         }

         switch (st) {
            case NONE:
               return (T) DefaultDataContainer.unBoundedDataContainer(
                     level, keyEquivalence);
            case UNORDERED:
            case LRU:

               if (configuration.eviction().type() == EvictionType.MEMORY) {
                  EntrySizeCalculator esc;
                  if (configuration.storeAsBinary().storeKeysAsBinary() &&
                          configuration.storeAsBinary().storeValuesAsBinary()) {
                     esc = new MarshalledValueEntrySizeCalculator();
                  } else {
                     esc = new PrimitiveEntrySizeCalculator();
                  }

                  return (T) DefaultDataContainer.boundedDataContainer(
                          level, thresholdSize, st, configuration.eviction().threadPolicy(), keyEquivalence,
                          esc);
               }
            case FIFO:
            case LIRS:
               return (T) DefaultDataContainer.boundedDataContainer(
                  level, thresholdSize, st, configuration.eviction().threadPolicy(), keyEquivalence,
                  configuration.eviction().type());
            default:
               throw new CacheConfigurationException("Unknown eviction strategy "
                        + configuration.eviction().strategy());
         }
      }
   }
}
