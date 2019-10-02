package org.infinispan.rest.search.entity;

import java.io.Serializable;

import org.infinispan.protostream.annotations.ProtoEnumValue;

/**
 * @since 9.2
 */
@SuppressWarnings("unused")
public enum Gender implements Serializable {
   @ProtoEnumValue(number = 1)
   MALE,
   @ProtoEnumValue(number = 2)
   FEMALE
}
