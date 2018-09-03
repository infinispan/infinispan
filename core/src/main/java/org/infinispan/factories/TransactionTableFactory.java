package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.impl.ComponentAlias;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryAwareTransactionTable;

/**
 * Factory for {@link org.infinispan.transaction.impl.TransactionTable} objects.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@DefaultFactoryFor(classes = {TransactionTable.class, org.infinispan.transaction.TransactionTable.class})
@SuppressWarnings("unused")
public class TransactionTableFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      if (componentName.equals(TransactionTable.class.getName())) {
         return ComponentAlias.of(org.infinispan.transaction.TransactionTable.class);
      }

      if (!configuration.transaction().transactionMode().isTransactional())
         return null;

      if (configuration.invocationBatching().enabled())
         return new TransactionTable();

      if (!configuration.transaction().useSynchronization()) {
         if (configuration.transaction().recovery().enabled()) {
            return new RecoveryAwareTransactionTable();
         } else {
            return new XaTransactionTable();
         }
      } else {
         return new TransactionTable();
      }
   }
}
