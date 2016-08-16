package org.infinispan.factories;

import org.infinispan.configuration.cache.CacheMode;
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
      if (cacheMode.isDistributed() || cacheMode.isReplicated()) {
         return componentType.cast(new ClusterExpirationManager<>());
      } else {
         return componentType.cast(new ExpirationManagerImpl<>());
      }
   }
}
