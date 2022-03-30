package org.infinispan.commons.tx;

import javax.transaction.Synchronization;
import javax.transaction.xa.XAResource;

/**
 * Converts {@link Synchronization} and {@link XAResource} to {@link AsyncSynchronization} and {@link AsyncXaResource}.
 *
 * @since 14.0
 */
public interface TransactionResourceConverter {

   /**
    * @param synchronization The {@link Synchronization} to convert.
    * @return An {@link AsyncSynchronization} instance of {@code synchronization}.
    */
   AsyncSynchronization convertSynchronization(Synchronization synchronization);

   /**
    * @param resource The {@link XAResource} to convert.
    * @return An {@link AsyncXaResource} instance of {@code resource}.
    */
   AsyncXaResource convertXaResource(XAResource resource);

}
