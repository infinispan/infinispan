package org.infinispan.factories;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.EntrySizeCalculator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.DataContainer;
import org.infinispan.container.DefaultDataContainer;
import org.infinispan.container.entries.MarshalledValueEntrySizeCalculator;
import org.infinispan.container.entries.PrimitiveEntrySizeCalculator;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.expiration.impl.ClusterExpirationManager;
import org.infinispan.expiration.impl.ExpirationManagerImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;

/**
 * Constructs the expiration manager
 *
 * @author William Burns
 * @since 8.0
 */
@DefaultFactoryFor(classes = ExpirationManager.class)
public class ExpirationManagerFactory extends AbstractNamedCacheComponentFactory implements
         AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      CacheMode cacheMode = configuration.clustering().cacheMode();
      if (cacheMode.isDistributed() || cacheMode.isReplicated() || cacheMode.isScattered()) {
         return componentType.cast(new ClusterExpirationManager<>());
      } else {
         return componentType.cast(new ExpirationManagerImpl<>());
      }
   }
}
