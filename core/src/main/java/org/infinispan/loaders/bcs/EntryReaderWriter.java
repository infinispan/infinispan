package org.infinispan.loaders.bcs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Helper for reading/writing entries into file.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class EntryReaderWriter {
   private static final int MAGIC = 0xBE11A61C;
   private static final boolean useMagic = false;
   public static final int HEADER_SIZE = 22 + (useMagic ? 4 : 0);

   public static class EntryHeader {
      private final int keyLength;
      private final int valueLength;
      private final long seqId;
      private final long expiration;

      public EntryHeader(ByteBuffer buffer) {
         if (useMagic) {
            if (buffer.getInt() != MAGIC) throw new IllegalStateException();
         }
         this.keyLength = buffer.getShort();
         this.valueLength = buffer.getInt();
         this.seqId = buffer.getLong();
         this.expiration = buffer.getLong();
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

      public int keyLength() {
         return keyLength;
      }

      @Override
      public String toString() {
         return String.format("[keyLength=%d, valueLength=%d, seqId=%d, expiration=%d]", keyLength, valueLength, seqId, expiration);
      }

      public int totalLength() {
         return keyLength + valueLength + HEADER_SIZE;
      }
   }

   public static EntryHeader readEntryHeader(FileProvider.Handle handle, long offset) throws IOException {
      ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
      if (read(handle, header, offset, HEADER_SIZE) < 0) return null;
      header.flip();
      try {
         return new EntryHeader(header);
      } catch (IllegalStateException e) {
         throw new IllegalStateException("Error reading from " + handle.getFileId() + ":" + offset);
      }
   }

   public static EntryHeader readNextEntryHeader(FileChannel fileChannel) throws IOException {
      ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
      if (!read(fileChannel, header, HEADER_SIZE)) {
         return null;
      }
      header.flip();
      return new EntryHeader(header);
   }

   public static byte[] readKey(FileProvider.Handle handle, EntryHeader header, long offset) throws IOException {
      byte[] key = new byte[header.keyLength];
      if (read(handle, ByteBuffer.wrap(key), offset + HEADER_SIZE, header.keyLength) < 0) return null;
      return key;
   }

   public static byte[] readNextKey(FileChannel fileChannel, EntryHeader header) throws IOException {
      byte[] key = new byte[header.keyLength];
      if (!read(fileChannel, ByteBuffer.wrap(key), header.keyLength)) return null;
      return key;
   }

   public static byte[] readValue(FileProvider.Handle handle, EntryHeader header, long offset) throws IOException {
      byte[] value = new byte[header.valueLength];
      if (read(handle, ByteBuffer.wrap(value), offset + HEADER_SIZE + header.keyLength, header.valueLength) < 0) return null;
      return value;
   }

   public static byte[] readNextValue(FileChannel fileChannel, EntryHeader header) throws IOException {
      byte[] value = new byte[header.valueLength];
      if (!read(fileChannel, ByteBuffer.wrap(value), header.valueLength)) return null;
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

   public static void writeEntry(FileChannel fileChannel, byte[] serializedKey, byte[] serializedValue, long seqId, long expiration) throws IOException {
      ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
      if (useMagic) {
         header.putInt(MAGIC);
      }
      header.putShort((short) serializedKey.length);
      header.putInt(serializedValue == null ? 0 : serializedValue.length);
      header.putLong(seqId);
      header.putLong(expiration);
      header.flip();
      write(fileChannel, header);
      write(fileChannel, ByteBuffer.wrap(serializedKey));
      if (serializedValue != null) {
         write(fileChannel, ByteBuffer.wrap(serializedValue));
      }
   }

   private static void write(FileChannel fileChannel, ByteBuffer buffer) throws IOException {
      while (buffer.hasRemaining()) fileChannel.write(buffer);
   }
}
