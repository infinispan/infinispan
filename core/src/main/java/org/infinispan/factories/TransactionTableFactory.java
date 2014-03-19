package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryAwareTransactionTable;

/**
 * Factory for {@link org.infinispan.transaction.impl.TransactionTable} objects.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@DefaultFactoryFor(classes = {TransactionTable.class})
@SuppressWarnings("unused")
public class TransactionTableFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType) {
      if (configuration.invocationBatching().enabled())
         return (T) new TransactionTable();

      if (!configuration.transaction().useSynchronization()) {
         if (configuration.transaction().recovery().enabled()) {
            return (T) new RecoveryAwareTransactionTable();
         } else {
            return (T) new XaTransactionTable();
         }
      } else return (T) new TransactionTable();

   }
}
