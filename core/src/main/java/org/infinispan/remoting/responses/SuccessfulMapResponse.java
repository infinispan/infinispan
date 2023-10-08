package org.infinispan.remoting.responses;

import java.util.Map;
import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_MAP_RESPONSE)
public class SuccessfulMapResponse<K, V> implements SuccessfulResponse<Map<K, V>> {

   @ProtoField(1)
   final MarshallableMap<K, V> map;

   @ProtoFactory
   static <K, V> SuccessfulMapResponse<K, V> protoFactory(MarshallableMap<K, V> map) {
      return map == null ? null : new SuccessfulMapResponse<>(map);
   }

   SuccessfulMapResponse(Map<K, V> map) {
      this.map = MarshallableMap.create(map);
   }

   SuccessfulMapResponse(MarshallableMap<K, V> map) {
      this.map = map;
   }

   public Map<K, V> getResponseValue() {
      return MarshallableMap.unwrap(map);
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      SuccessfulMapResponse<?, ?> that = (SuccessfulMapResponse<?, ?>) o;
      return Objects.equals(map, that.map);
   }

   @Override
   public int hashCode() {
      return Objects.hashCode(map);
   }

   @Override
   public String toString() {
      return "SuccessfulMapResponse{" +
            "map=" + map +
            '}';
   }
}
