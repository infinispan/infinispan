package org.infinispan.transaction.lookup;

import javax.transaction.TransactionSynchronizationRegistry;

/**
 * @author Stuart Douglas
 *
 * If we are in a JTA transaction that tx.commit has already been called and
 * we are invoked as part of a interposed synchronization, we need to use the TransactionSynchronizationRegistry
 * to register any further needed synchronizations.  This interface is how we will lookup the
 * TransactionSynchronizationRegistry.  Although, in most cases, we will already have it
 * injected via some other means (avoiding a JNDI lookup).
 *
 * See ISPN-1168 for more details.
 *
 */
public interface TransactionSynchronizationRegistryLookup {

   /**
    * Returns a new TransactionSynchronizationRegistry.
    *
    * @throws Exception if lookup failed
    */
   TransactionSynchronizationRegistry getTransactionSynchronizationRegistry() throws Exception;

}
