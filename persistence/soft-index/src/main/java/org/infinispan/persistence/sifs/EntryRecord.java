package org.infinispan.persistence.sifs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Helper for reading/writing entries into file.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class EntryRecord {

   private EntryHeader header;
   private byte[] key;
   private byte[] value;
   private EntryMetadata meta;

   EntryRecord(EntryHeader header, byte[] key) {
      this.header = header;
      this.key = key;
   }

   public EntryHeader getHeader() {
      return header;
   }

   public byte[] getKey() {
      return key;
   }

   public byte[] getMetadata() {
      return meta == null ? null : meta.getBytes();
   }

   public byte[] getValue() {
      return value;
   }

   public long getCreated() {
      return meta == null ? -1 : meta.getCreated();
   }

   public long getLastUsed() {
      return meta == null ? -1 :meta.getLastUsed();
   }

   public EntryRecord loadMetadataAndValue(FileProvider.Handle handle, int offset) throws IOException {
      if (header.metadataLength() > 0) {
         meta = readMetadata(handle, header, offset);
      }
      value = readValue(handle, header, offset);
      return this;
   }

   public static EntryHeader readEntryHeader(FileProvider.Handle handle, long offset) throws IOException {
      ByteBuffer header = ByteBuffer.allocate(EntryHeader.HEADER_SIZE);
      if (read(handle, header, offset, EntryHeader.HEADER_SIZE) < 0) {
         return null;
      }
      header.flip();
      try {
         return new EntryHeader(header);
      } catch (IllegalStateException e) {
         throw new IllegalStateException("Error reading from " + handle.getFileId() + ":" + offset);
      }
   }

   public static byte[] readKey(FileProvider.Handle handle, EntryHeader header, long offset) throws IOException {
      byte[] key = new byte[header.keyLength()];
      if (read(handle, ByteBuffer.wrap(key), offset + EntryHeader.HEADER_SIZE, header.keyLength()) < 0) {
         return null;
      }
      return key;
   }

   public static EntryMetadata readMetadata(FileProvider.Handle handle, EntryHeader header, long offset) throws IOException {
      assert header.metadataLength() > 0;
      offset += EntryHeader.HEADER_SIZE + header.keyLength();
      int metaLength = header.metadataLength() - EntryMetadata.TIMESTAMP_BYTES;
      assert metaLength > 0;
      byte[] metadata = new byte[metaLength];
      if (read(handle, ByteBuffer.wrap(metadata), offset, metaLength) < 0) {
         throw new IllegalStateException("End of file reached when reading metadata on "
               + handle.getFileId() + ":" + offset + ": " + header);
      }

      offset += metaLength;
      ByteBuffer buffer = ByteBuffer.allocate(EntryMetadata.TIMESTAMP_BYTES);
      if (read(handle, buffer, offset, EntryMetadata.TIMESTAMP_BYTES) < 0) {
         throw new IllegalStateException("End of file reached when reading timestamps on "
               + handle.getFileId() + ":" + offset + ": " + header);
      }
      buffer.flip();
      return new EntryMetadata(metadata, buffer.getLong(), buffer.getLong());
   }

   public static byte[] readValue(FileProvider.Handle handle, EntryHeader header, long offset) throws IOException {
      assert header.valueLength() > 0;
      byte[] value = new byte[header.valueLength()];
      if (read(handle, ByteBuffer.wrap(value), offset + EntryHeader.HEADER_SIZE + header.keyLength() + header.metadataLength(), header.valueLength()) < 0) {
         throw new IllegalStateException("End of file reached when reading metadata on "
               + handle.getFileId() + ":" + offset + ": " + header);
      }
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

   public static void writeEntry(FileChannel fileChannel, byte[] serializedKey, EntryMetadata metadata, byte[] serializedValue,
                                 long seqId, long expiration) throws IOException {
      ByteBuffer header = ByteBuffer.allocate(EntryHeader.HEADER_SIZE);
      if (EntryHeader.useMagic) {
         header.putInt(EntryHeader.MAGIC);
      }
      header.putShort((short) serializedKey.length);
      header.putShort(metadata == null ? (short) 0 : (short) metadata.length());
      header.putInt(serializedValue == null ? 0 : serializedValue.length);
      header.putLong(seqId);
      header.putLong(expiration);
      header.flip();
      write(fileChannel, header);
      write(fileChannel, ByteBuffer.wrap(serializedKey));
      if (metadata != null) {
         write(fileChannel, ByteBuffer.wrap(metadata.getBytes()));
         writeTimestamps(fileChannel, metadata.getCreated(), metadata.getLastUsed());
      }
      if (serializedValue != null) {
         write(fileChannel, ByteBuffer.wrap(serializedValue));
      }
   }

   public static void writeEntry(FileChannel fileChannel, org.infinispan.commons.io.ByteBuffer serializedKey,
                                 org.infinispan.commons.io.ByteBuffer serializedMetadata, org.infinispan.commons.io.ByteBuffer serializedValue,
                                 long seqId, long expiration, long created, long lastUsed) throws IOException {
      ByteBuffer header = ByteBuffer.allocate(EntryHeader.HEADER_SIZE);
      if (EntryHeader.useMagic) {
         header.putInt(EntryHeader.MAGIC);
      }
      header.putShort((short) serializedKey.getLength());
      header.putShort(EntryMetadata.size(serializedMetadata));
      header.putInt(serializedValue == null ? 0 : serializedValue.getLength());
      header.putLong(seqId);
      header.putLong(expiration);
      header.flip();
      write(fileChannel, header);
      write(fileChannel, ByteBuffer.wrap(serializedKey.getBuf(), serializedKey.getOffset(), serializedKey.getLength()));
      if (serializedMetadata != null) {
         write(fileChannel, ByteBuffer.wrap(serializedMetadata.getBuf(), serializedMetadata.getOffset(), serializedMetadata.getLength()));
         writeTimestamps(fileChannel, created, lastUsed);
      }
      if (serializedValue != null) {
         write(fileChannel, ByteBuffer.wrap(serializedValue.getBuf(), serializedValue.getOffset(), serializedValue.getLength()));
      }
   }

   private static void writeTimestamps(FileChannel fileChannel, long created, long lastUsed) throws IOException {
      ByteBuffer buffer = ByteBuffer.allocate(EntryMetadata.TIMESTAMP_BYTES);
      buffer.putLong(created);
      buffer.putLong(lastUsed);
      buffer.flip();
      write(fileChannel, buffer);
   }

   private static void write(FileChannel fileChannel, ByteBuffer buffer) throws IOException {
      while (buffer.hasRemaining()) fileChannel.write(buffer);
   }
}
