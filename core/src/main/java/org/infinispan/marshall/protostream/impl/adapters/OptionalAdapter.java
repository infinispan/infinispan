package org.infinispan.marshall.protostream.impl.adapters;

import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(Optional.class)
@ProtoTypeId(ProtoStreamTypeIds.ADAPTER_OPTIONAL)
public class OptionalAdapter {

   @ProtoFactory
   static Optional<?> create(MarshallableObject<?> value) {
      return Optional.ofNullable(value);
   }

   @ProtoField(1)
   MarshallableObject<?> getValue(Optional<?> optional) {
      return MarshallableObject.create(optional.orElse(null));
   }
}
