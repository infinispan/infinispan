package org.infinispan.server.hotrod.tx.table;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * A key used in the global transaction table.
 * <p>
 * The global transaction table is a replicated cache. This key contains the cache name and the transactions' {@link
 * XidImpl}.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
@Proto
@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_CACHE_XID)
public record CacheXid(ByteString cacheName, XidImpl xid) {

   public boolean sameXid(XidImpl other) {
      return xid.equals(other);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      CacheXid cacheXid = (CacheXid) o;
      return cacheName.equals(cacheXid.cacheName) && xid.equals(cacheXid.xid);
   }

   @Override
   public String toString() {
      return "CacheXid{" +
            "cacheName=" + cacheName +
            ", xid=" + xid +
            '}';
   }
}
