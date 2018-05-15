package org.infinispan.factories;

import javax.transaction.TransactionManager;

import org.infinispan.commons.CacheException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.transaction.tm.BatchModeTransactionManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Uses a number of mechanisms to retrieve a transaction manager.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @since 4.0
 */
@DefaultFactoryFor(classes = {TransactionManager.class})
public class TransactionManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   private static final Log log = LogFactory.getLog(TransactionManagerFactory.class);

   @Override
   public <T> T construct(Class<T> componentType) {
      if (!configuration.transaction().transactionMode().isTransactional()) {
         return null;
      }

      // See if we had a TransactionManager injected into our config
      TransactionManager transactionManager = null;

      TransactionManagerLookup lookup = configuration.transaction().transactionManagerLookup();
      try {
         if (lookup != null) {
            componentRegistry.wireDependencies(lookup);
            transactionManager = lookup.getTransactionManager();
         }
      } catch (Exception e) {
         log.couldNotInstantiateTransactionManager(e);
      }

      if (transactionManager == null && configuration.invocationBatching().enabled()) {
         log.usingBatchModeTransactionManager();
         transactionManager = BatchModeTransactionManager.getInstance();
      }

      if (transactionManager == null) {
         throw new CacheException("This is transactional cache but no transaction manager could be found. " +
                                        "Configure the transaction manager lookup properly.");
      }

      return componentType.cast(transactionManager);
   }
}
