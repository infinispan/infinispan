package org.infinispan.xsite;

import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoName;

@ProtoName("BringSiteOnlineResponse")
public enum BringSiteOnlineResponse {
   @ProtoEnumValue(number = 1)
   NO_SUCH_SITE,
   @ProtoEnumValue(number = 2)
   ALREADY_ONLINE,
   @ProtoEnumValue(number = 3)
   BROUGHT_ONLINE
}
