package org.infinispan.topology;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * RebalancingStatus.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@ProtoTypeId(ProtoStreamTypeIds.REBALANCE_STATUS)
@Proto
public enum RebalancingStatus {
   SUSPENDED,
   PENDING,
   IN_PROGRESS,
   COMPLETE
}
