package org.infinispan.marshall.persistence.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Util;
import org.infinispan.persistence.spi.MarshalledValue;

/**
 * A marshallable object that can be used by our internal store implementations to store values, metadata and timestamps.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class MarshalledValueImpl implements MarshalledValue {

   static final MarshalledValue EMPTY = new MarshalledValueImpl();

   ByteBuffer valueBytes;
   ByteBuffer metadataBytes;
   long created;
   long lastUsed;

   MarshalledValueImpl(ByteBuffer valueBytes, ByteBuffer metadataBytes, long created, long lastUsed) {
      this.valueBytes = valueBytes;
      this.metadataBytes = metadataBytes;
      this.created = created;
      this.lastUsed = lastUsed;
   }

   MarshalledValueImpl() {}

   @Override
   public ByteBuffer getValueBytes() {
      return valueBytes;
   }

   @Override
   public ByteBuffer getMetadataBytes() {
      return metadataBytes;
   }

   @Override
   public long getCreated() {
      return created;
   }

   @Override
   public long getLastUsed() {
      return lastUsed;
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

   public static class Externalizer implements AdvancedExternalizer<MarshalledValueImpl> {

      @Override
      public void writeObject(ObjectOutput output, MarshalledValueImpl e) throws IOException {
         output.writeObject(e.valueBytes);
         output.writeObject(e.metadataBytes);
         output.writeLong(e.created);
         output.writeLong(e.lastUsed);
      }

      @Override
      public MarshalledValueImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         MarshalledValueImpl e = new MarshalledValueImpl();
         e.valueBytes = (ByteBuffer) input.readObject();
         e.metadataBytes = (ByteBuffer) input.readObject();
         e.created = input.readLong();
         e.lastUsed = input.readLong();
         return e;
      }

      @Override
      public Set<Class<? extends MarshalledValueImpl>> getTypeClasses() {
         return Util.asSet(MarshalledValueImpl.class);
      }

      @Override
      public Integer getId() {
         return Ids.MARSHALLED_VALUE;
      }
   }
}
