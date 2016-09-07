package org.infinispan.query.affinity;

import java.util.List;

import org.hibernate.search.backend.LuceneWork;

/**
 * Callback for indexing backend exception handling.
 *
 * @since 9.0
 */
interface OperationFailedHandler {

   /**
    * Called when operations failed in the backend.
    *
    * @param failingOperations Operations that failed to be applied to the index.
    */
   void operationsFailed(List<LuceneWork> failingOperations, Throwable cause);
}
