package org.infinispan.util.concurrent.locks;

import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;

/**
 * Simple interface to extract all the keys that may need to be locked for transactional commands.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface TransactionalRemoteLockCommand extends RemoteLockCommand {

   /**
    * It creates the transaction context.
    *
    * @return the {@link TxInvocationContext}.
    */
   TxInvocationContext<?> createContext(ComponentRegistry componentRegistry);
}
