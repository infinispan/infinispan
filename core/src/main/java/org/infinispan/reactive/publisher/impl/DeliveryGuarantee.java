package org.infinispan.reactive.publisher.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Enumeration defining the possible delivery guarantees for entries.
 * @author wburns
 * @since 10.0
 */
@Proto
@ProtoTypeId(ProtoStreamTypeIds.DELIVERY_GUARANTEE)
public enum DeliveryGuarantee {
   /**
    * The least strict guarantee that ensures that data is never read more than once, but may be missed. This guarantee
    * is most performant as it never requires retrying data or returning extra data for the sake of consistency. However,
    * under a stable topology this will return the same results as {@link #EXACTLY_ONCE}.
    */
   AT_MOST_ONCE,
   /**
    * The in between guarantee that provides a view of all data, but may return duplicates during a toplogy change. This
    * guarantee does not send identity values, but instead will retry an operation, most likely returning duplicates.
    * However, under a stable topology this will return the same results as {@link #EXACTLY_ONCE}.
    */
   AT_LEAST_ONCE,
   /**
    * The most strict guarantee that guarantees that an entry is seen exactly one time in results. This is the most
    * expensive guarantee as it may require copying identity values to the originator (ie. keys) to ensure that a
    * value is not returned more than once for a given key.
    */
   EXACTLY_ONCE,
   ;

   private static final DeliveryGuarantee[] CACHED_VALUES = DeliveryGuarantee.values();

   public static DeliveryGuarantee valueOf(int index) {
      return CACHED_VALUES[index];
   }
}
