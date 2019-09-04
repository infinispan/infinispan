package org.infinispan.marshall.persistence.impl;

import java.util.Objects;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.persistence.spi.MarshalledValue;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A marshallable object that can be used by our internal store implementations to store values, metadata and timestamps.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MARSHALLED_VALUE_IMPL)
public class MarshalledValueImpl implements MarshalledValue {

   static final MarshalledValue EMPTY = new MarshalledValueImpl((ByteBuffer) null, null, -1, -1);

   private final ByteBuffer valueBytes;
   private final ByteBuffer metadataBytes;
   private final long created;
   private final long lastUsed;

   @ProtoFactory
   MarshalledValueImpl(byte[] value, byte[] metadata, long created, long lastUsed) {
      this(new ByteBufferImpl(value), new ByteBufferImpl(metadata), created, lastUsed);
   }

   MarshalledValueImpl(ByteBuffer valueBytes, ByteBuffer metadataBytes, long created, long lastUsed) {
      this.valueBytes = valueBytes;
      this.metadataBytes = metadataBytes;
      this.created = created;
      this.lastUsed = lastUsed;
   }

   @ProtoField(number = 1, name = "value")
   byte[] getValue() {
      return valueBytes == null ? Util.EMPTY_BYTE_ARRAY : MarshallUtil.toByteArray(valueBytes);
   }

   @ProtoField(number = 2, name = "metadata")
   byte[] getMetadata() {
      return metadataBytes == null ? Util.EMPTY_BYTE_ARRAY : MarshallUtil.toByteArray(metadataBytes);
   }

   @Override
   @ProtoField(number = 3, name = "created", defaultValue = "-1")
   public long getCreated() {
      return created;
   }

   @Override
   @ProtoField(number = 4, name = "lastUsed", defaultValue = "-1")
   public long getLastUsed() {
      return lastUsed;
   }

   @Override
   public ByteBuffer getValueBytes() {
      return valueBytes;
   }

   @Override
   public ByteBuffer getMetadataBytes() {
      return metadataBytes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MarshalledValueImpl that = (MarshalledValueImpl) o;
      return created == that.created &&
            lastUsed == that.lastUsed &&
            Objects.equals(valueBytes, that.valueBytes) &&
            Objects.equals(metadataBytes, that.metadataBytes);
   }

   @Override
   public int hashCode() {
      return Objects.hash(valueBytes, metadataBytes, created, lastUsed);
   }

   @Override
   public String toString() {
      return "MarshalledValueImpl{" +
            "valueBytes=" + valueBytes +
            ", metadataBytes=" + metadataBytes +
            ", created=" + created +
            ", lastUsed=" + lastUsed +
            '}';
   }
}
