package org.infinispan.query.dsl.embedded.testdomain;

import org.infinispan.protostream.annotations.ProtoEnumValue;

public enum Currency {
   @ProtoEnumValue(number = 1)
   EUR,
   @ProtoEnumValue(number = 2)
   GBP,
   @ProtoEnumValue(number = 3)
   USD,
   @ProtoEnumValue(number = 4)
   BRL
}
