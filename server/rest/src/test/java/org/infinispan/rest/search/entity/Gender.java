package org.infinispan.rest.search.entity;

import org.infinispan.protostream.annotations.ProtoEnumValue;

/**
 * @since 9.2
 */
public enum Gender {
   @ProtoEnumValue(number = 0)
   MALE,
   @ProtoEnumValue(number = 1)
   FEMALE
}
