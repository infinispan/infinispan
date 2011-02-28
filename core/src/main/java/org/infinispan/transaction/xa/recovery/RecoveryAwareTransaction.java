package org.infinispan.transaction.xa.recovery;

import org.infinispan.transaction.xa.CacheTransaction;

/**
 * Base interface fro transactions that are holders of recovery information.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public interface RecoveryAwareTransaction extends CacheTransaction {

   public boolean isPrepared();

   public void setPrepared(boolean prepared);
}
