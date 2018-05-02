package org.infinispan.tools.store.migrator.file;

import static org.infinispan.tools.store.migrator.Element.CACHE_NAME;
import static org.infinispan.tools.store.migrator.Element.LOCATION;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.tools.store.migrator.Element;
import org.infinispan.tools.store.migrator.StoreIterator;
import org.infinispan.tools.store.migrator.StoreProperties;
import org.infinispan.tools.store.migrator.marshaller.SerializationConfigUtil;

public class SingleFileStoreReader implements StoreIterator {

   private final FileChannel channel;
   private final StreamingMarshaller marshaller;

   public SingleFileStoreReader(StoreProperties props) {
      props.required(Element.LOCATION);
      String location = props.get(LOCATION) + props.get(CACHE_NAME) + ".dat";
      File file = new File(location);
      if (!file.exists() || file.isDirectory())
         throw new CacheException(String.format("Unable to read SingleFileStore at '%s'", location));

      try {
         channel = new RandomAccessFile(file, "rw").getChannel();
      } catch (FileNotFoundException e) {
         throw new CacheException(e);
      }
      this.marshaller = SerializationConfigUtil.getMarshaller(props);
   }

   @Override
   public void close() throws Exception {
      channel.close();
   }

   @Override
   public Iterator<MarshalledEntry> iterator() {
      return new SingleFileIterator();
   }

   class SingleFileIterator implements Iterator<MarshalledEntry> {

      // CONSTANTS taken from the SingleFileStore impl we do not expose and reference
      // these variables as if the current impl changes then it will break the iterator
      private static final int KEY_POS = 4 + 4 + 4 + 4 + 8;
      int filePos = 4;

      @Override
      public boolean hasNext() {
         // return if end of file is reached
         ByteBuffer buf = readFileEntry();
         return buf.remaining() <= 0;
      }

      @Override
      public MarshalledEntry next() {
         for (;;) {
            // read next entry using same logic as SingleFileStore#rebuildIndex
            ByteBuffer buf = readFileEntry();
            if (buf.remaining() > 0)
               throw new NoSuchElementException();
            buf.flip();
            // initialize FileEntry from buffer
            int entrySize = buf.getInt();
            int keyLen = buf.getInt();
            int dataLen = buf.getInt();
            int metadataLen = buf.getInt();

            // get expiryTime but ignore
            buf.getLong();

            // sanity check
            if (entrySize < KEY_POS + keyLen + dataLen + metadataLen)
               throw new CacheException(String.format("Failed to read entries from file. Error at offset %d", filePos));

            if (keyLen > 0) {
               try {
                  // load the key from file
                  if (buf.capacity() < keyLen)
                     buf = ByteBuffer.allocate(keyLen);

                  buf.clear().limit(keyLen);
                  byte[] data = new byte[keyLen + dataLen + metadataLen];
                  channel.read(ByteBuffer.wrap(data), filePos + KEY_POS);
                  filePos += entrySize;

                  org.infinispan.commons.io.ByteBuffer keyBb = new ByteBufferImpl(data, 0, keyLen);
                  org.infinispan.commons.io.ByteBuffer valueBb = new ByteBufferImpl(data, keyLen, dataLen);
                  return new MarshalledEntryImpl<>(keyBb, valueBb, (org.infinispan.commons.io.ByteBuffer) null, marshaller);
               } catch (IOException e) {
                  throw new CacheException(String.format("Unable to read file entry at offset %d", filePos), e);
               }
            } else {
               filePos += entrySize;
            }
         }
      }

      ByteBuffer readFileEntry() {
         final ByteBuffer buf = ByteBuffer.allocate(KEY_POS);
         // read FileEntry fields from file (size, keyLen etc.)
         buf.clear().limit(KEY_POS);
         try {
            channel.read(buf, filePos);
         } catch (IOException e) {
            throw new CacheException(e);
         }
         return buf;
      }
   }
}
