package org.infinispan.transaction.xa.recovery;

import org.infinispan.transaction.xa.CacheTransaction;

/**
 * Base interface for transactions that are holders of recovery information.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public interface RecoveryAwareTransaction extends CacheTransaction {

   boolean isPrepared();

   void setPrepared(boolean prepared);
}
