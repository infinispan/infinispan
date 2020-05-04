package org.infinispan.transaction.xa.recovery;

import java.util.Objects;

import org.infinispan.commons.tx.XidImpl;

/**
 * This makes sure that only xids pertaining to a certain cache are being returned when needed. This is required as the
 * {@link RecoveryManagerImpl#registerInDoubtTransaction(RecoveryAwareRemoteTransaction)} is shared between different
 * RecoveryManagers/caches.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public final class RecoveryInfoKey {
   final XidImpl xid;

   final String cacheName;

   public RecoveryInfoKey(XidImpl xid, String cacheName) {
      this.xid = xid;
      this.cacheName = cacheName;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RecoveryInfoKey recoveryInfoKey = (RecoveryInfoKey) o;

      return Objects.equals(cacheName, recoveryInfoKey.cacheName) && Objects.equals(xid, recoveryInfoKey.xid);
   }

   @Override
   public int hashCode() {
      int result = xid != null ? xid.hashCode() : 0;
      result = 31 * result + (cacheName != null ? cacheName.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "RecoveryInfoKey{" +
            "xid=" + xid +
            ", cacheName='" + cacheName + '\'' +
            '}';
   }
}
