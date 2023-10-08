package org.infinispan.topology;

import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Dan Berindei
 * @since 7.1
 */
@Proto
@ProtoTypeId(ProtoStreamTypeIds.MANAGER_STATUS_RESPONSE)
public record ManagerStatusResponse(Map<String, CacheStatusResponse> caches, boolean rebalancingEnabled) {
}
