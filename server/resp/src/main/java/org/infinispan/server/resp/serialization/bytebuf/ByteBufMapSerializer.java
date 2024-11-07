package org.infinispan.server.resp.serialization.bytebuf;

import java.util.Map;

import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.serialization.NestedResponseSerializer;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.SerializationHint;

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
final class ByteBufMapSerializer implements NestedResponseSerializer<Map<Object, Object>, ByteBufPool, SerializationHint.KeyValueHint> {
   static final ByteBufMapSerializer INSTANCE = new ByteBufMapSerializer();

   @Override
   public void accept(Map<Object, Object> map, ByteBufPool alloc, SerializationHint.KeyValueHint hint) {
      // RESP: %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
      ByteBufferUtils.writeNumericPrefix(RespConstants.MAP, map.size(), alloc);
      ByteBufResponseWriter writer = new ByteBufResponseWriter(alloc);
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
         hint.key().serialize(entry.getKey(), writer);
         hint.value().serialize(entry.getValue(), writer);
      }
   }

   @Override
   public boolean test(Object object) {
      // Accept any map.
      return object instanceof Map<?,?>;
   }
}
