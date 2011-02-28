package org.infinispan.transaction.xa.recovery;

import javax.transaction.xa.Xid;

/**
 * This makes sure that only xids pertaining to a certain cache are being returned when needed. This is required as the
 * {@link RecoveryManagerImpl#preparedTransactions} is shared between different RecoveryManagers/caches.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public final class RecoveryInfoKey {
   final Xid xid;

   final String cacheName;

   public RecoveryInfoKey(Xid xid, String cacheName) {
      this.xid = xid;
      this.cacheName = cacheName;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RecoveryInfoKey recoveryInfoKey = (RecoveryInfoKey) o;

      if (cacheName != null ? !cacheName.equals(recoveryInfoKey.cacheName) : recoveryInfoKey.cacheName != null)
         return false;
      if (xid != null ? !xid.equals(recoveryInfoKey.xid) : recoveryInfoKey.xid != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = xid != null ? xid.hashCode() : 0;
      result = 31 * result + (cacheName != null ? cacheName.hashCode() : 0);
      return result;
   }

   public boolean sameCacheName(String cacheName) {
      return this.cacheName.equals(cacheName);
   }

   @Override
   public String toString() {
      return "RecoveryInfoKey{" +
            "xid=" + xid +
            ", cacheName='" + cacheName + '\'' +
            '}';
   }
}
