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

   private final EntryHeader header;
   private final byte[] key;
   private byte[] value;
   private EntryMetadata meta;
   private byte[] internalMetadata;

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

   public byte[] getInternalMetadata() {
      return internalMetadata;
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

   public EntryRecord loadMetadataAndValue(FileProvider.Handle handle, int offset, boolean saveValue) throws IOException {
      loadMetadata(handle, offset);
      byte[] readValue = null;
      if (value == null) {
         readValue = readValue(handle, header, offset);
         if (saveValue) {
            value = readValue;
         }
      }
      if (internalMetadata == null && header.internalMetadataLength() > 0) {
         internalMetadata = readInternalMetadata(handle, header, offset);
      }
      if (value == null) {
         assert !saveValue;
         assert readValue != null;
         EntryRecord copyRecord = new EntryRecord(header, key);
         copyRecord.meta = meta;
         copyRecord.internalMetadata = internalMetadata;
         copyRecord.value = readValue;
         return copyRecord;
      }
      return this;
   }

   public EntryRecord loadMetadata(FileProvider.Handle handle, int offset) throws IOException {
      if (meta == null && header.metadataLength() > 0) {
         meta = readMetadata(handle, header, offset);
      }
      return this;
   }

   public static EntryHeader readEntryHeader(FileProvider.Handle handle, long offset) throws IOException {
      ByteBuffer header = ByteBuffer.allocate(EntryHeader.HEADER_SIZE_11_0);
      if (read(handle, header, offset, EntryHeader.HEADER_SIZE_11_0) < 0) {
         return null;
      }
      header.flip();
      try {
         return new EntryHeader(header);
      } catch (IllegalStateException e) {
         throw new IllegalStateException("Error reading from " + handle.getFileId() + ":" + offset, e);
      }
   }

   public static EntryHeader read10_1EntryHeader(FileProvider.Handle handle, long offset) throws IOException {
      ByteBuffer header = ByteBuffer.allocate(EntryHeader.HEADER_SIZE_10_1);
      if (read(handle, header, offset, EntryHeader.HEADER_SIZE_10_1) < 0) {
         return null;
      }
      header.flip();
      try {
         return new EntryHeader(header, true);
      } catch (IllegalStateException e) {
         throw new IllegalStateException("Error reading from " + handle.getFileId() + ":" + offset, e);
      }
   }

   public static byte[] readKey(FileProvider.Handle handle, EntryHeader header, long offset) throws IOException {
      byte[] key = new byte[header.keyLength()];
      if (read(handle, ByteBuffer.wrap(key), offset + header.getHeaderLength(), header.keyLength()) < 0) {
         return null;
      }
      return key;
   }

   public static EntryMetadata readMetadata(FileProvider.Handle handle, EntryHeader header, long offset) throws IOException {
      assert header.metadataLength() > 0;
      offset += header.getHeaderLength() + header.keyLength();
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

   public static byte[] readInternalMetadata(FileProvider.Handle handle, EntryHeader header, long offset) throws IOException {
      final int length = header.internalMetadataLength();
      assert length > 0;
      offset += header.getHeaderLength() + header.keyLength() + header.metadataLength() + header.valueLength();
      byte[] metadata = new byte[length];
      if (read(handle, ByteBuffer.wrap(metadata), offset, length) < 0) {
         throw new IllegalStateException("End of file reached when reading internal metadata on "
               + handle.getFileId() + ":" + offset + ": " + header);
      }
      return metadata;
   }

   public static byte[] readValue(FileProvider.Handle handle, EntryHeader header, long offset) throws IOException {
      assert header.valueLength() > 0;
      byte[] value = new byte[header.valueLength()];
      if (read(handle, ByteBuffer.wrap(value), offset + header.getHeaderLength() + header.keyLength() + header.metadataLength(), header.valueLength()) < 0) {
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

   public static void writeEntry(FileChannel fileChannel, ByteBuffer reusedBuffer, byte[] serializedKey, EntryMetadata metadata, byte[] serializedValue,
                                 byte[] serializedInternalMetadata, long seqId, long expiration) throws IOException {
      assert reusedBuffer.limit() == EntryHeader.HEADER_SIZE_11_0;
      assert reusedBuffer.position() == 0;
      EntryHeader.writeHeader(reusedBuffer, (short) serializedKey.length, metadata == null ? 0 : (short) metadata.length(),
            serializedValue == null ? 0 : serializedValue.length,
            serializedInternalMetadata == null ? 0 : (short) serializedInternalMetadata.length,
            seqId, expiration);
      reusedBuffer.flip();
      write(fileChannel, reusedBuffer);
      reusedBuffer.position(0);
      write(fileChannel, ByteBuffer.wrap(serializedKey));
      if (metadata != null) {
         write(fileChannel, ByteBuffer.wrap(metadata.getBytes()));
         writeTimestamps(fileChannel, reusedBuffer, metadata.getCreated(), metadata.getLastUsed());
      }
      if (serializedValue != null) {
         write(fileChannel, ByteBuffer.wrap(serializedValue));
      }
      if (serializedInternalMetadata != null) {
         write(fileChannel, ByteBuffer.wrap(serializedInternalMetadata));
      }
   }

   static void writeEntry(FileChannel fileChannel, ByteBuffer reusedBuffer, ByteBuffer serializedKey,
                                 ByteBuffer serializedMetadata,
                                 ByteBuffer serializedInternalMetadata,
                                 ByteBuffer serializedValue,
                                 long seqId, long expiration, long created, long lastUsed) throws IOException {
      assert reusedBuffer.limit() == EntryHeader.HEADER_SIZE_11_0;
      assert reusedBuffer.position() == 0;
      EntryHeader.writeHeader(reusedBuffer, (short) serializedKey.remaining(), EntryMetadata.size(serializedMetadata),
            serializedValue == null ? 0 : serializedValue.remaining(),
            serializedInternalMetadata == null ? 0 : (short) serializedInternalMetadata.remaining(),
            seqId, expiration);
      reusedBuffer.flip();
      write(fileChannel, reusedBuffer);
      reusedBuffer.position(0);
      write(fileChannel, serializedKey);
      if (serializedMetadata != null) {
         write(fileChannel, serializedMetadata);
         writeTimestamps(fileChannel, reusedBuffer, created, lastUsed);
      }
      if (serializedValue != null) {
         write(fileChannel, serializedValue);
      }
      if (serializedInternalMetadata != null) {
         write(fileChannel, serializedInternalMetadata);
      }
   }

   private static void writeTimestamps(FileChannel fileChannel, ByteBuffer reusedBuffer, long created, long lastUsed) throws IOException {
      assert reusedBuffer.position() == 0;
      int previousLimit = reusedBuffer.limit();
      assert previousLimit >= EntryMetadata.TIMESTAMP_BYTES;
      reusedBuffer.putLong(created);
      reusedBuffer.putLong(lastUsed);
      reusedBuffer.flip();
      write(fileChannel, reusedBuffer);

      // Reset the buffer to what it was before
      reusedBuffer.position(0);
      reusedBuffer.limit(previousLimit);
   }

   private static void write(FileChannel fileChannel, ByteBuffer buffer) throws IOException {
      while (buffer.hasRemaining()) fileChannel.write(buffer);
   }
}
