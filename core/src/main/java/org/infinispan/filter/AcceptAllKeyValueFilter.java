package org.infinispan.filter;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A key value filter that accepts all entries found.
 * <p>
 * <b>This filter should be used carefully as it may cause the operation to perform very slowly
 * as all entries are accepted.</b>
 *
 * @author wburns
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.ACCEPT_ALL_KEY_VALUE_FILTER)
public final class AcceptAllKeyValueFilter implements KeyValueFilter<Object, Object> {

   private AcceptAllKeyValueFilter() {
   }

   private static class StaticHolder {
      private static final AcceptAllKeyValueFilter INSTANCE = new AcceptAllKeyValueFilter();
   }

   @ProtoFactory
   public static AcceptAllKeyValueFilter getInstance() {
      return StaticHolder.INSTANCE;
   }

   @Override
   public boolean accept(Object key, Object value, Metadata metadata) {
      return true;
   }
}
