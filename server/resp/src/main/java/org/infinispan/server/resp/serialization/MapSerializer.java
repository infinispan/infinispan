package org.infinispan.server.resp.serialization;

import java.util.Map;

import org.infinispan.server.resp.ByteBufPool;

/**
 * Represent an unordered sequence of key-value tuples.
 *
 * <p>
 * The prefix is a percent sign, followed by the base-10 representation of the number key-value tuples. Each key and value
 * follow the representation of the respective type. Therefore, the map is heterogeneous in its elements.
 * </p>
 *
 * @since 15.0
 * @author José Bolina
 */
final class MapSerializer implements NestedResponseSerializer<Map<Object, Object>, SerializationHint.KeyValueHint> {
   static final MapSerializer INSTANCE = new MapSerializer();

   @Override
   public void accept(Map<Object, Object> map, ByteBufPool alloc, SerializationHint.KeyValueHint hint) {
      // RESP: %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
      ByteBufferUtils.writeNumericPrefix(RespConstants.MAP, map.size(), alloc);

      for (Map.Entry<Object, Object> entry : map.entrySet()) {
         hint.key().serialize(entry.getKey(), alloc);
         hint.value().serialize(entry.getValue(), alloc);
      }
   }

   @Override
   public boolean test(Object object) {
      // Accept any map.
      return object instanceof Map<?,?>;
   }
}
