package org.infinispan.executors;

import java.util.concurrent.ExecutorService;

/**
 * Marker interface for executor service wrappers that delegate to an underlying executor.
 *
 * @author William Burns
 * @since 16.2
 */
public interface WrappedExecutorService extends ExecutorService {
   /**
    * Returns the underlying executor service that this wrapper delegates to.
    *
    * @return the wrapped executor service
    */
   ExecutorService unwrap();
}
