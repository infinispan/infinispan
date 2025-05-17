package org.infinispan.tools.store.migrator.file;

import static org.infinispan.tools.store.migrator.Element.CACHE_NAME;
import static org.infinispan.tools.store.migrator.Element.LOCATION;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.persistence.file.SingleFileStore;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.tools.store.migrator.Element;
import org.infinispan.tools.store.migrator.StoreIterator;
import org.infinispan.tools.store.migrator.StoreProperties;
import org.infinispan.tools.store.migrator.marshaller.SerializationConfigUtil;

public class SingleFileStoreReader implements StoreIterator {

   private final FileChannel channel;
   private final MarshallableEntryFactory<?,?> entryFactory;
   private final FileEntryReader reader;

   public SingleFileStoreReader(StoreProperties props) {
      props.required(Element.LOCATION);
      String filename = props.get(CACHE_NAME) + ".dat";
      Path path = Path.of(props.get(LOCATION)).resolve(filename);
      String location = path.toString();
      File file = path.toFile();
      if (!file.exists() || file.isDirectory())
         throw new CacheException(String.format("Unable to read SingleFileStore at '%s'", location));

      try {
         channel = new RandomAccessFile(file, "rw").getChannel();
         byte[] magicBytes = new byte[SingleFileStore.MAGIC_11_0.length];
         if (channel.read(ByteBuffer.wrap(magicBytes)) <= 0) {
            throw new CacheException(String.format("File at \"%s\" is corrupted.", location));
         }
         if (Arrays.equals(magicBytes, SingleFileStore.MAGIC_BEFORE_11)) {
            this.reader = new OldReader();
         } else if (Arrays.equals(magicBytes, SingleFileStore.MAGIC_11_0) || Arrays.equals(magicBytes, SingleFileStore.MAGIC_LATEST)) {
            this.reader = new NewReader();
         } else {
            throw new CacheException(String.format("File at \"%s\" is corrupted. Unexpected magic number.", location));
         }
      } catch (IOException e) {
         throw new CacheException(e);
      }

      this.entryFactory = SerializationConfigUtil.getEntryFactory(props);
   }

   @Override
   public void close() throws Exception {
      channel.close();
   }

   @Override
   public Iterator<MarshallableEntry> iterator() {
      return new SingleFileIterator();
   }

   class SingleFileIterator implements Iterator<MarshallableEntry> {
      int filePos = 4; //skip 4 bytes magic number

      @Override
      public boolean hasNext() {
         // return if end of file is reached
         return reader.hasNext(channel, filePos);
      }

      @Override
      public MarshallableEntry next() {
         for (;;) {
            // read next entry using same logic as SingleFileStore#rebuildIndex
            Entry entry = reader.read(channel, filePos);
            if (entry == null)
               throw new NoSuchElementException();

            // sanity check
            if (entry.size < entry.keyLen + entry.dataLen + entry.metadataLen + entry.internalMetadataLen)
               throw new CacheException(String.format("Failed to read entries from file. Error at offset %d", filePos));

            if (entry.keyLen > 0) {
               try {
                  // load the key and value from file
                  byte[] data = new byte[entry.keyLen + entry.dataLen];
                  if (channel.read(ByteBuffer.wrap(data), filePos + reader.keyOffset()) <= 0) {
                     throw new CacheException(String.format("Failed to read entries from file. Error at offset %d", filePos));
                  }
                  filePos += entry.size;

                  org.infinispan.commons.io.ByteBuffer keyBb = ByteBufferImpl.create(data, 0, entry.keyLen);
                  org.infinispan.commons.io.ByteBuffer valueBb = ByteBufferImpl.create(data, entry.keyLen, entry.dataLen);
                  return entryFactory.create(keyBb, valueBb);
               } catch (IOException e) {
                  throw new CacheException(String.format("Unable to read file entry at offset %d", filePos), e);
               }
            } else {
               filePos += entry.size;
            }
         }
      }
   }

   private static class Entry {
      final int size;
      final int keyLen;
      final int dataLen;
      final int metadataLen;
      final int internalMetadataLen;

      private Entry(int size, int keyLen, int dataLen, int metadataLen, int internalMetadataLen) {
         this.size = size;
         this.keyLen = keyLen;
         this.dataLen = dataLen;
         this.metadataLen = metadataLen;
         this.internalMetadataLen = internalMetadataLen;
      }
   }

   private interface FileEntryReader {

      Entry read(FileChannel channel, int filePosition);

      boolean hasNext(FileChannel channel, int filePosition);

      int keyOffset();

   }

   private abstract static class BaseReader implements FileEntryReader {

      private final int keyOffset;
      final ByteBuffer byteBuffer;

      BaseReader(int keyOffset) {
         this.keyOffset = keyOffset;
         this.byteBuffer = ByteBuffer.allocate(keyOffset);
      }

      @Override
      public final Entry read(FileChannel channel, int filePosition) {
         //in both magics numbers, the size, key length and value length are in the same position in the file
         byteBuffer.clear().limit(keyOffset);
         try {
            if (channel.read(byteBuffer, filePosition) <= 0) {
               return null;
            }
         } catch (IOException e) {
            throw new CacheException(e);
         }
         byteBuffer.flip();
         Entry entry = readEntry();
         return entry;
      }

      @Override
      public final boolean hasNext(FileChannel channel, int filePosition) {
         byteBuffer.clear().limit(keyOffset);
         try {
            return channel.read(byteBuffer, filePosition) > 0;
         } catch (IOException e) {
            throw new CacheException(e);
         }
      }

      @Override
      public final int keyOffset() {
         return keyOffset;
      }

      abstract Entry readEntry();
   }

   private static class OldReader extends BaseReader {

      OldReader() {
         super(SingleFileStore.KEY_POS_BEFORE_11);
      }

      @Override
      Entry readEntry() {
         int entrySize = byteBuffer.getInt();
         int keyLen = byteBuffer.getInt();
         int dataLen = byteBuffer.getInt();
         int metadataLen = byteBuffer.getInt();
         return new Entry(entrySize, keyLen, dataLen, metadataLen, 0);
      }
   }

   private static class NewReader extends BaseReader {

      NewReader() {
         super(SingleFileStore.KEY_POS_11_0);
      }

      @Override
      Entry readEntry() {
         int entrySize = byteBuffer.getInt();
         int keyLen = byteBuffer.getInt();
         int dataLen = byteBuffer.getInt();
         int metadataLen = byteBuffer.getInt();
         int internalMetadataLen = byteBuffer.getInt();
         return new Entry(entrySize, keyLen, dataLen, metadataLen, internalMetadataLen);
      }
   }
}
