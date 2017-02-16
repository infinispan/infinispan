package org.infinispan.transaction.impl;

import org.infinispan.transaction.xa.CacheTransaction;

/**
 * Base class for both Sync and XAResource enlistment adapters.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public abstract class AbstractEnlistmentAdapter {
   private final int hashCode;

   public AbstractEnlistmentAdapter(CacheTransaction cacheTransaction) {
      hashCode = preComputeHashCode(cacheTransaction);
   }

   public AbstractEnlistmentAdapter() {
      hashCode = 31;
   }

   /**
    * Invoked by TransactionManagers, make sure it's an efficient implementation.
    * System.identityHashCode(x) is NOT an efficient implementation.
    */
   @Override
   public final int hashCode() {
      return this.hashCode;
   }

   private static int preComputeHashCode(final CacheTransaction cacheTx) {
      return 31 + cacheTx.hashCode();
   }
}
