package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryAwareTransactionTable;

/**
 * Factory for {@link org.infinispan.transaction.TransactionTable} objects.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@DefaultFactoryFor(classes = {TransactionTable.class})
public class TransactionTableFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType) {
      if (!configuration.isUseSynchronizationForTransactions()) {
         if (configuration.isTransactionRecoveryEnabled()) {
            return (T) new RecoveryAwareTransactionTable();
         } else {
            return (T) new XaTransactionTable();
         }
      } else return (T) new TransactionTable();

   }
}
