package org.infinispan.xsite.status;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * The site state.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_SITE_STATE)
@Proto
public enum SiteState {
   NOT_FOUND,
   ONLINE,
   OFFLINE
}
