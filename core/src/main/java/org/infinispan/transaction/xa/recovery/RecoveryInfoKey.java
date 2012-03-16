/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
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
