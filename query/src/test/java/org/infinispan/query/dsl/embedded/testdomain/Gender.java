package org.infinispan.query.dsl.embedded.testdomain;

import org.infinispan.protostream.annotations.ProtoEnumValue;

public enum Gender {
   @ProtoEnumValue(number = 1)
   MALE,
   @ProtoEnumValue(number = 2)
   FEMALE
}
