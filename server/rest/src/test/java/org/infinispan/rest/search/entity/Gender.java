package org.infinispan.rest.search.entity;

import org.infinispan.protostream.annotations.ProtoEnumValue;

/**
 * @since 9.2
 */
public enum Gender {
   @ProtoEnumValue(number = 1)
   MALE,
   @ProtoEnumValue(number = 2)
   FEMALE
}
