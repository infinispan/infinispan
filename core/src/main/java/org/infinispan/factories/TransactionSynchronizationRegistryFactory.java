package org.infinispan.factories;

import javax.transaction.TransactionSynchronizationRegistry;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.transaction.lookup.TransactionSynchronizationRegistryLookup;

/**
 * Factory for the TransactionSynchronizationRegistry
 *
 * @author Stuart Douglas
 */
@DefaultFactoryFor(classes = {TransactionSynchronizationRegistry.class})
public class TransactionSynchronizationRegistryFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType) {
      // See if we had a TransactionSynchronizationRegistry injected into our config
      TransactionSynchronizationRegistryLookup lookup = configuration.transaction().transactionSynchronizationRegistryLookup();

      try {
         if (lookup != null) {
            return componentType.cast(lookup.getTransactionSynchronizationRegistry());
         }
      }
      catch (Exception e) {
         throw new CacheConfigurationException("failed obtaining TransactionSynchronizationRegistry", e);
      }
      return null;
   }
}
