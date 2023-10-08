package org.infinispan.xsite.status;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * The return value of {@link TakeOfflineManager#bringSiteOnline(String)}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_BRING_ONLINE_RESPONSE)
@Proto
public enum BringSiteOnlineResponse {
   BSOR_NO_SUCH_SITE,
   BSOR_ALREADY_ONLINE,
   BSOR_BROUGHT_ONLINE
}
