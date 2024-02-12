package org.infinispan.server.resp.filter;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.server.resp.RespTypes;

/**
 * Filter based on the {@link RespTypes}.
 * <p>
 * The filter does not convert the value type V.
 *
 * @param <V>: The type of the value.
 * @since 15.0
 */
@ProtoName("RespTypeFilterConverter")
public class RespTypeFilterConverter<V> extends AbstractKeyValueFilterConverter<byte[], V, V> {

   private final RespTypes type;

   @ProtoFactory
   public RespTypeFilterConverter(byte ordinal) {
      this.type = RespTypes.fromOrdinal(ordinal);
   }

   @ProtoField(number = 1, defaultValue = "0")
   byte ordinal() {
      return (byte) type.ordinal();
   }

   @Override
   public V filterAndConvert(byte[] key, V value, Metadata metadata) {
      return typeMatches(value) ? value : null;
   }

   private boolean typeMatches(V value) {
      return type == RespTypes.fromValueClass(value.getClass());
   }

   @Override
   public MediaType format() {
      return null;
   }
}
