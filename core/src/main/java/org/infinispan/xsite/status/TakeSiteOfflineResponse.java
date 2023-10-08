package org.infinispan.xsite.status;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * The return value of {@link TakeOfflineManager#takeSiteOffline(String)}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_TAKE_OFFLINE_RESPONSE)
@Proto
public enum TakeSiteOfflineResponse {
   TSOR_NO_SUCH_SITE,
   TSOR_ALREADY_OFFLINE,
   TSOR_TAKEN_OFFLINE
}
