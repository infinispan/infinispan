package org.infinispan.xsite;

import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoName;

@ProtoName("TakeSiteOfflineResponse")
public enum TakeSiteOfflineResponse {
   @ProtoEnumValue(number = 1)
   NO_SUCH_SITE,
   @ProtoEnumValue(number = 2)
   ALREADY_OFFLINE,
   @ProtoEnumValue(number = 3)
   TAKEN_OFFLINE
}
