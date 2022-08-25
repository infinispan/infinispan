package org.infinispan.persistence.sifs;

import java.nio.ByteBuffer;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class EntryHeader {
   private static final byte MAGIC = 0x01;
   /* 1 byte  - magic key
    * 2 bytes - key length
    * 2 bytes - metadata length
    * 4 bytes - value length
    * 8 bytes - seq id
    * 8 bytes - expiration time
    */
   static final int HEADER_SIZE_10_1 = 24;
   /* 1 byte  - magic key
    * 2 bytes - key length
    * 2 bytes - metadata length
    * 4 bytes - value length
    * 2 bytes - internal metadata length
    * 8 bytes - seq id
    * 8 bytes - expiration time
    */
   static final int HEADER_SIZE_11_0 = 27;

   private final int keyLength;
   private final int valueLength;
   private final int metadataLength;
   private final long seqId;
   private final long expiration;
   private final int internalMetadataLength;
   private final int headerLength;

   public EntryHeader(ByteBuffer buffer) {
      this(buffer, false);
   }

   public EntryHeader(ByteBuffer buffer, boolean oldFormat) {
      byte magicByte;
      if (!oldFormat && (magicByte = buffer.get()) != MAGIC) {
         throw new IllegalStateException("Magic byte was: " + magicByte);
      }
      this.keyLength = buffer.getShort();
      this.metadataLength = buffer.getShort();
      this.valueLength = buffer.getInt();
      this.internalMetadataLength = oldFormat ? 0 : buffer.getShort();
      this.seqId = buffer.getLong();
      this.expiration = buffer.getLong();
      this.headerLength = oldFormat ? HEADER_SIZE_10_1 : HEADER_SIZE_11_0;
   }

   public int keyLength() {
      return keyLength;
   }

   public int metadataLength() {
      return metadataLength;
   }

   public int internalMetadataLength() {
      return internalMetadataLength;
   }

   public int valueLength() {
      return valueLength;
   }

   public long seqId() {
      return seqId;
   }

   public long expiryTime() {
      return expiration;
   }

   public int getHeaderLength() {
      return headerLength;
   }

   @Override
   public String toString() {
      return String.format("[keyLength=%d, valueLength=%d, metadataLength=%d, internalMetadataLength=%d,seqId=%d, expiration=%d]", keyLength, valueLength, metadataLength, internalMetadataLength, seqId, expiration);
   }

   public int totalLength() {
      return keyLength + metadataLength + internalMetadataLength + valueLength + headerLength;
   }

   public static void writeHeader(ByteBuffer buf, short keyLength, short metadataLength, int valueLength, short internalMetadataLength, long seqId, long expiration) {
      buf.put(EntryHeader.MAGIC);
      buf.putShort(keyLength);
      buf.putShort(metadataLength);
      buf.putInt(valueLength);
      buf.putShort(internalMetadataLength);
      buf.putLong(seqId);
      buf.putLong(expiration);
   }
}
