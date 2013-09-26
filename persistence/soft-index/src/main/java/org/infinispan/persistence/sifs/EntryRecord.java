package org.infinispan.persistence.sifs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Helper for reading/writing entries into file.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class EntryRecord {

   private EntryHeader header;
   private byte[] key;
   private byte[] metadata;
   private byte[] value;

   public EntryRecord(EntryHeader header, byte[] key, byte[] metadata, byte[] value) {
      this.header = header;
      this.key = key;
      this.metadata = metadata;
      this.value = value;
   }

   public EntryHeader getHeader() {
      return header;
   }

   public byte[] getKey() {
      return key;
   }

   public byte[] getMetadata() {
      return metadata;
   }

   public byte[] getValue() {
      return value;
   }

   public EntryRecord loadMetadataAndValue(FileProvider.Handle handle, int offset) throws IOException {
      int metadataOffset = offset + EntryHeader.HEADER_SIZE + header.keyLength();
      if (header.metadataLength() > 0) {
         metadata = new byte[header.metadataLength()];
         if (read(handle, ByteBuffer.wrap(metadata), metadataOffset, header.metadataLength()) < 0) return null;
      }
      value = new byte[header.valueLength()];
      if (read(handle, ByteBuffer.wrap(value), metadataOffset + header.metadataLength(), header.valueLength()) < 0) return null;
      return this;
   }

   public static EntryHeader readEntryHeader(FileProvider.Handle handle, long offset) throws IOException {
      ByteBuffer header = ByteBuffer.allocate(EntryHeader.HEADER_SIZE);
      if (read(handle, header, offset, EntryHeader.HEADER_SIZE) < 0) return null;
      header.flip();
      try {
         return new EntryHeader(header);
      } catch (IllegalStateException e) {
         throw new IllegalStateException("Error reading from " + handle.getFileId() + ":" + offset);
      }
   }

   public static byte[] readKey(FileProvider.Handle handle, EntryHeader header, long offset) throws IOException {
      byte[] key = new byte[header.keyLength()];
      if (read(handle, ByteBuffer.wrap(key), offset + EntryHeader.HEADER_SIZE, header.keyLength()) < 0) return null;
      return key;
   }

   public static byte[] readMetadata(FileProvider.Handle handle, EntryHeader header, long offset) throws IOException {
      byte[] metadata = new byte[header.metadataLength()];
      if (read(handle, ByteBuffer.wrap(metadata), offset + EntryHeader.HEADER_SIZE + header.keyLength(), header.metadataLength()) < 0) return null;
      return metadata;
   }

   public static byte[] readValue(FileProvider.Handle handle, EntryHeader header, long offset) throws IOException {
      byte[] value = new byte[header.valueLength()];
      if (read(handle, ByteBuffer.wrap(value), offset + EntryHeader.HEADER_SIZE + header.keyLength() + header.metadataLength(), header.valueLength()) < 0) return null;
      return value;
   }

   private static int read(FileProvider.Handle handle, ByteBuffer buffer, long position, int length) throws IOException {
      int read = 0;
      do {
         int newRead = handle.read(buffer, position + read);
         if (newRead < 0) {
            return -1;
         }
         read += newRead;
      } while (read < length);
      return read;
   }

   private static boolean read(FileChannel fileChannel, ByteBuffer buffer, int length) throws IOException {
      int read = 0;
      do {
         int newRead = fileChannel.read(buffer);
         if (newRead < 0) {
            return false;
         }
         read += newRead;
      } while (read < length);
      return true;
   }

   public static void writeEntry(FileChannel fileChannel, byte[] serializedKey, byte[] serializedMetadata, byte[] serializedValue, long seqId, long expiration) throws IOException {
      ByteBuffer header = ByteBuffer.allocate(EntryHeader.HEADER_SIZE);
      if (EntryHeader.useMagic) {
         header.putInt(EntryHeader.MAGIC);
      }
      header.putShort((short) serializedKey.length);
      header.putShort(serializedMetadata == null ? (short) 0 : (short) serializedMetadata.length);
      header.putInt(serializedValue == null ? 0 : serializedValue.length);
      header.putLong(seqId);
      header.putLong(expiration);
      header.flip();
      write(fileChannel, header);
      write(fileChannel, ByteBuffer.wrap(serializedKey));
      if (serializedMetadata != null) {
         write(fileChannel, ByteBuffer.wrap(serializedMetadata));
      }
      if (serializedValue != null) {
         write(fileChannel, ByteBuffer.wrap(serializedValue));
      }
   }

   public static void writeEntry(FileChannel fileChannel, org.infinispan.commons.io.ByteBuffer serializedKey, org.infinispan.commons.io.ByteBuffer serializedMetadata, org.infinispan.commons.io.ByteBuffer serializedValue, long seqId, long expiration) throws IOException {
      ByteBuffer header = ByteBuffer.allocate(EntryHeader.HEADER_SIZE);
      if (EntryHeader.useMagic) {
         header.putInt(EntryHeader.MAGIC);
      }
      header.putShort((short) serializedKey.getLength());
      header.putShort(serializedMetadata == null ? (short) 0 : (short) serializedMetadata.getLength());
      header.putInt(serializedValue == null ? 0 : serializedValue.getLength());
      header.putLong(seqId);
      header.putLong(expiration);
      header.flip();
      write(fileChannel, header);
      write(fileChannel, ByteBuffer.wrap(serializedKey.getBuf(), serializedKey.getOffset(), serializedKey.getLength()));
      if (serializedMetadata != null) {
         write(fileChannel, ByteBuffer.wrap(serializedMetadata.getBuf(), serializedMetadata.getOffset(), serializedMetadata.getLength()));
      }
      if (serializedValue != null) {
         write(fileChannel, ByteBuffer.wrap(serializedValue.getBuf(), serializedValue.getOffset(), serializedValue.getLength()));
      }
   }

   private static void write(FileChannel fileChannel, ByteBuffer buffer) throws IOException {
      while (buffer.hasRemaining()) fileChannel.write(buffer);
   }
}
