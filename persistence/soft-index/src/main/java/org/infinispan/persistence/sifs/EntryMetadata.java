package org.infinispan.persistence.sifs;

import org.infinispan.commons.io.ByteBuffer;

/**
 * Object to hold metadata bytes and timestamps.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class EntryMetadata {

   static final int TIMESTAMP_BYTES = 8 + 8;

   private final byte[] metadataBytes;
   private final long created;
   private final long lastUsed;

   public EntryMetadata(byte[] metadataBytes, long created, long lastUsed) {
      this.metadataBytes = metadataBytes;
      this.created = created;
      this.lastUsed = lastUsed;
   }

   public byte[] getBytes() {
      return metadataBytes;
   }

   public long getCreated() {
      return created;
   }

   public long getLastUsed() {
      return lastUsed;
   }

   public int length() {
      return metadataBytes.length +  TIMESTAMP_BYTES;
   }

   static short size(ByteBuffer buffer) {
      return (short) (buffer == null ? 0 : buffer.getLength() + TIMESTAMP_BYTES);
   }
}
