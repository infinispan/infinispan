package org.infinispan.factories;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.expiration.impl.ClusterExpirationManager;
import org.infinispan.expiration.impl.ExpirationManagerImpl;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.expiration.impl.TxClusterExpirationManager;
import org.infinispan.factories.annotations.DefaultFactoryFor;

/**
 * Constructs the expiration manager
 *
 * @author William Burns
 * @since 8.0
 */
@DefaultFactoryFor(classes = InternalExpirationManager.class)
public class ExpirationManagerFactory extends AbstractNamedCacheComponentFactory implements
         AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public Object construct(String componentName) {
      CacheMode cacheMode = configuration.clustering().cacheMode();
      if (cacheMode.needsStateTransfer()) {
         if (configuration.transaction().transactionMode().isTransactional()) {
            return new TxClusterExpirationManager<>();
         }
         return new ClusterExpirationManager<>();
      } else {
         return new ExpirationManagerImpl<>();
      }
   }
}
