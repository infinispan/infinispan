package org.infinispan.server.hotrod.tx.table.functions;

import java.util.function.Predicate;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.hotrod.tx.table.CacheXid;

/**
 * A {@link Predicate} to filter the {@link CacheXid} by its {@link XidImpl}.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
@Proto
@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_XID_PREDICATE)
public record XidPredicate(XidImpl xid) implements Predicate<CacheXid> {

   @Override
   public boolean test(CacheXid cacheXid) {
      return cacheXid.sameXid(xid);
   }
}
