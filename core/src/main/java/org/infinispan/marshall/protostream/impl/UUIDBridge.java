package org.infinispan.marshall.protostream.impl;

import java.util.UUID;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoBridgeFor;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.descriptors.Type;

@ProtoTypeId(ProtoStreamTypeIds.UUID)
@ProtoBridgeFor(UUID.class)
public class UUIDBridge {

   @ProtoFactory
   UUID create(Long mostSigBits, Long leastSigBits, Long mostSigBitsFixed, Long leastSigBitsFixed) {
      if (mostSigBits == null)
         return new UUID(mostSigBitsFixed, leastSigBitsFixed);

      // Create the UUID using the old fields
      return new UUID(mostSigBits, leastSigBits);
   }

   @ProtoField(number = 1, type = Type.UINT64)
   Long getMostSigBits(UUID uuid) {
      return null;
   }

   @ProtoField(number = 2, type = Type.UINT64)
   Long getLeastSigBits(UUID uuid) {
      return null;
   }

   @ProtoField(number = 3, type = Type.FIXED64, defaultValue = "0")
   Long getMostSigBitsFixed(UUID uuid) {
      return uuid.getMostSignificantBits();
   }

   @ProtoField(number = 4, type = Type.FIXED64, defaultValue = "0")
   Long getLeastSigBitsFixed(UUID uuid) {
      return uuid.getLeastSignificantBits();
   }
}
