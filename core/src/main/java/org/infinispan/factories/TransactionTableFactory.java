package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.transaction.xa.TransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryEnabledTransactionTable;

/**
 * Factory for {@link org.infinispan.transaction.xa.TransactionTable} objects.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@DefaultFactoryFor(classes = {TransactionTable.class})
public class TransactionTableFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType) {
      if (configuration.isTransactionRecoveryEnabled()) {
         return (T) new RecoveryEnabledTransactionTable();
      } else {
         return (T) new TransactionTable();
      }
   }
}
