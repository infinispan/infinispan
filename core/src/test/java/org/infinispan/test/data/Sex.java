package org.infinispan.test.data;

import org.infinispan.protostream.annotations.ProtoEnumValue;

public enum Sex {
   @ProtoEnumValue(number = 1)
   FEMALE,
   @ProtoEnumValue(number = 2)
   MALE,
}
