package org.infinispan.test.data;

import org.infinispan.protostream.annotations.ProtoEnumValue;

public enum Sex {
   @ProtoEnumValue(number = 0)
   FEMALE,
   @ProtoEnumValue(number = 1)
   MALE,
}
