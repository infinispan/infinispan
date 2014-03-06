package org.infinispan.factories;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.container.DataContainer;
import org.infinispan.container.DefaultDataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
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

         switch (st) {
            case NONE:
               return (T) DefaultDataContainer.unBoundedDataContainer(
                     level, keyEquivalence);
            case UNORDERED:
            case LRU:
            case FIFO:
            case LIRS:
               int maxEntries = configuration.eviction().maxEntries();
               //handle case when < 0 value signifies unbounded container 
               if(maxEntries < 0) {
                   return (T) DefaultDataContainer.unBoundedDataContainer(
                         level, keyEquivalence);
               }

               EvictionThreadPolicy policy = configuration.eviction().threadPolicy();

               return (T) DefaultDataContainer.boundedDataContainer(
                  level, maxEntries, st, policy, keyEquivalence);
            default:
               throw new CacheConfigurationException("Unknown eviction strategy "
                        + configuration.eviction().strategy());
         }
      }
   }
}
