package org.infinispan.tools.store.migrator.file;

import static org.infinispan.tools.store.migrator.Element.LOCATION;

import java.io.File;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.persistence.sifs.EntryHeader;
import org.infinispan.persistence.sifs.EntryRecord;
import org.infinispan.persistence.sifs.FileProvider;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.tools.store.migrator.Element;
import org.infinispan.tools.store.migrator.StoreIterator;
import org.infinispan.tools.store.migrator.StoreProperties;
import org.infinispan.tools.store.migrator.marshaller.SerializationConfigUtil;

public class SoftIndexFileStoreIterator implements StoreIterator {

   private final MarshallableEntryFactory entryFactory;
   private final String location;

   public SoftIndexFileStoreIterator(StoreProperties props) {
      props.required(Element.LOCATION);
      String location = props.get(LOCATION);
      File file = new File(location);
      if (!file.exists() || !file.isDirectory())
         throw new CacheException(String.format("Unable to read directory at '%s'", location));

      this.location = location;
      this.entryFactory = SerializationConfigUtil.getEntryFactory(props);
   }

   @Override
   public void close() {
   }

   @Override
   public Iterator<MarshallableEntry> iterator() {
      return new SoftIndexIterator(location);
   }

   class SoftIndexIterator implements Iterator<MarshallableEntry> {

      final FileProvider fileProvider;
      final Iterator<Integer> iterator;
      FileProvider.Handle handle;
      int file = -1;
      int offset = 0;

      SoftIndexIterator(String location) {
         this.fileProvider = new FileProvider(location, 1000);
         this.iterator = fileProvider.getFileIterator();
      }

      @Override
      public boolean hasNext() {
         return file > -1 || iterator.hasNext();
      }

      @Override
      public MarshallableEntry next() {
         try {
            while (hasNext()) {
               if (file < 0) {
                  file = iterator.next();
                  handle = fileProvider.getFile(file);
               }

               for (; ; ) {
                  EntryHeader header = EntryRecord.readEntryHeader(handle, offset);
                  if (header == null) {
                     handle.close();
                     file = -1;
                     break; // end of file;
                  }

                  if (header.valueLength() > 0) {
                     byte[] serializedKey = EntryRecord.readKey(handle, header, offset);
                     byte[] serializedValue = EntryRecord.readValue(handle, header, offset);

                     offset += header.totalLength();
                     if (EntryRecord.readEntryHeader(handle, offset) == null) {
                        // We have reached the end of the file, so we must reset fileIndex in case !iterator.hasNext()
                        handle.close();
                        file = -1;
                     }
                     return entryFactory.create(new ByteBufferImpl(serializedKey), new ByteBufferImpl(serializedValue));
                  }
                  offset += header.totalLength();
               }
            }
            throw new NoSuchElementException();
         } catch (Exception e) {
            throw new CacheException(e);
         }
      }
   }
}
