package org.infinispan.transaction.xa.recovery;

import org.infinispan.commons.tx.XidImpl;

/**
 * This makes sure that only xids pertaining to a certain cache are being returned when needed. This is required as the
 * {@link RecoveryManagerImpl#registerInDoubtTransaction(RecoveryAwareRemoteTransaction)} is shared between different
 * RecoveryManagers/caches.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public record RecoveryInfoKey(XidImpl xid, String cacheName) {
}
