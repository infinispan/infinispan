package org.infinispan.persistence.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.persistence.Store;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.Flowable;

/**
 * A filesystem-based implementation of a {@link org.infinispan.persistence.spi.AdvancedLoadWriteStore}. This file store
 * stores cache values in a single file <tt>&lt;location&gt;/&lt;cache name&gt;.dat</tt>,
 * keys and file positions are kept in memory.
 * <p/>
 * Note: this CacheStore implementation keeps keys and file positions in memory!
 * The current implementation needs about 100 bytes per cache entry, plus the
 * memory for the key objects.
 * <p/>
 * So, the space taken by this cache store is both the space in the file
 * itself plus the in-memory index with the keys and their file positions.
 * With this in mind and to avoid the cache store leading to
 * OutOfMemoryExceptions, you can optionally configure the maximum number
 * of entries to maintain in this cache store, which affects both the size
 * of the file and the size of the in-memory index. However, setting this
 * maximum limit results in older entries in the cache store to be eliminated,
 * and hence, it only makes sense configuring a maximum limit if Infinispan
 * is used as a cache where loss of data in the cache store does not lead to
 * data loss, and data can be recomputed or re-queried from the original data
 * source.
 * <p/>
 * This class is fully thread safe, yet allows for concurrent load / store
 * of individual cache entries.
 *
 * @author Karsten Blees
 * @author Mircea Markus
 * @since 6.0
 */
@Store
@ConfiguredBy(SingleFileStoreConfiguration.class)
public class SingleFileStore<K, V> implements AdvancedLoadWriteStore<K, V> {
   private static final Log log = LogFactory.getLog(SingleFileStore.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final byte[] MAGIC = new byte[]{'F', 'C', 'S', '1'};
   private static final byte[] ZERO_INT = {0, 0, 0, 0};
   private static final int KEYLEN_POS = 4;
   private static final int KEY_POS = 4 + 4 + 4 + 4 + 8;
   private static final int SMALLEST_ENTRY_SIZE = 128;

   private SingleFileStoreConfiguration configuration;

   protected InitializationContext ctx;

   private FileChannel channel;
   private Map<K, FileEntry> entries;
   private SortedSet<FileEntry> freeList;
   private long filePos = MAGIC.length;
   private File file;
   private float fragmentationFactor = .75f;
   // Prevent clear() from truncating the file after a write() allocated the entry but before it wrote the data
   private ReadWriteLock resizeLock = new ReentrantReadWriteLock();
   private TimeService timeService;

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
      this.timeService = ctx.getTimeService();
   }

   @Override
   public void start() {
      try {
         // open the data file
         String location = configuration.location();
         if (location == null || location.trim().length() == 0)
            location = "Infinispan-SingleFileStore";

         file = new File(location, ctx.getCache().getName() + ".dat");
         if (!file.exists()) {
            File dir = file.getParentFile();
            if (!dir.mkdirs() && !dir.exists()) {
               throw log.directoryCannotBeCreated(dir.getAbsolutePath());
            }
         }
         channel = new RandomAccessFile(file, "rw").getChannel();

         // initialize data structures. Only use LinkedHashMap (LRU) for entries when cache store is bounded
         Map<K, FileEntry> entryMap = configuration.maxEntries() > 0 ?
               new LinkedHashMap<>(16, 0.75f, true) :
               new HashMap<>();
         entries = Collections.synchronizedMap(entryMap);
         freeList = Collections.synchronizedSortedSet(new TreeSet<FileEntry>());

         // check file format and read persistent state if enabled for the cache
         byte[] header = new byte[MAGIC.length];
         if (channel.read(ByteBuffer.wrap(header), 0) == MAGIC.length && Arrays.equals(MAGIC, header)) {
            rebuildIndex();
            processFreeEntries();
         }
         else
            clear(); // otherwise (unknown file format or no preload) just reset the file

         // Initialize the fragmentation factor
         fragmentationFactor = configuration.fragmentationFactor();
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   @Override
   public void stop() {
      try {
         if (channel != null) {
            log.tracef("Stopping store %s, size = %d, file size = %d", ctx.getCache().getName(), entries.size(), channel.size());

            // reset state
            channel.close();
            channel = null;
            entries = null;
            freeList = null;
            filePos = MAGIC.length;
         }
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   @Override
   public boolean isAvailable() {
      return file.exists();
   }

   /**
    * Rebuilds the in-memory index from file.
    */
   private void rebuildIndex() throws Exception {
      ByteBuffer buf = ByteBuffer.allocate(KEY_POS);
      for (; ; ) {
         // read FileEntry fields from file (size, keyLen etc.)
         buf.clear().limit(KEY_POS);
         channel.read(buf, filePos);
         // return if end of file is reached
         if (buf.remaining() > 0)
            return;
         buf.flip();

         // initialize FileEntry from buffer
         int entrySize = buf.getInt();
         int keyLen = buf.getInt();
         int dataLen = buf.getInt();
         int metadataLen = buf.getInt();
         long expiryTime = buf.getLong();
         FileEntry fe = new FileEntry(filePos, entrySize, keyLen, dataLen, metadataLen, expiryTime);

         // sanity check
         if (fe.size < KEY_POS + fe.keyLen + fe.dataLen + fe.metadataLen) {
            throw log.errorReadingFileStore(file.getPath(), filePos);
         }

         // update file pointer
         filePos += fe.size;

         // check if the entry is used or free
         if (fe.keyLen > 0) {
            // load the key from file
            if (buf.capacity() < fe.keyLen)
               buf = ByteBuffer.allocate(fe.keyLen);

            buf.clear().limit(fe.keyLen);
            channel.read(buf, fe.offset + KEY_POS);

            // deserialize key and add to entries map
            // Marshaller should allow for provided type return for safety
            K key = (K) ctx.getMarshaller().objectFromByteBuffer(buf.array(), 0, fe.keyLen);
            entries.put(key, fe);
         } else {
            // add to free list
            freeList.add(fe);
         }
      }
   }

   /**
    * The base class implementation calls {@link #load(Object)} for this, we can do better because
    * we keep all keys in memory.
    */
   @Override
   public boolean contains(Object key) {
      FileEntry entry = entries.get(key);
      return entry != null && !entry.isExpired(timeService.wallClockTime());
   }

   /**
    * Allocates the requested space in the file.
    *
    * @param len requested space
    * @return allocated file position and length as FileEntry object
    */
   private FileEntry allocate(int len) {
      synchronized (freeList) {
         // lookup a free entry of sufficient size
         SortedSet<FileEntry> candidates = freeList.tailSet(new FileEntry(0, len));
         for (Iterator<FileEntry> it = candidates.iterator(); it.hasNext(); ) {
            FileEntry free = it.next();
            // ignore entries that are still in use by concurrent readers
            if (free.isLocked())
               continue;

            // There's no race condition risk between locking the entry on
            // loading and checking whether it's locked (or store allocation),
            // because for the entry to be lockable, it needs to be in the
            // entries collection, in which case it's not in the free list.
            // The only way an entry can be found in the free list is if it's
            // been removed, and to remove it, lock on "entries" needs to be
            // acquired, which is also a pre-requisite for loading data.

            // found one, remove from freeList
            it.remove();
            return allocateExistingEntry(free, len);
         }

         // no appropriate free section available, append at end of file
         FileEntry fe = new FileEntry(filePos, len);
         filePos += len;
         if (trace) log.tracef("New entry allocated at %d:%d, %d free entries, file size is %d", fe.offset, fe.size, freeList.size(), filePos);
         return fe;
      }
   }

   private FileEntry allocateExistingEntry(FileEntry free, int len) {
      int remainder = free.size - len;
      // If the entry is quite bigger than configured threshold, then split it
      if ((remainder >= SMALLEST_ENTRY_SIZE) && (len <= (free.size * fragmentationFactor))) {
         try {
            // Add remainder of the space as a fileEntry
            FileEntry newFreeEntry = new FileEntry(free.offset + len, remainder);
            addNewFreeEntry(newFreeEntry);
            FileEntry newEntry = new FileEntry(free.offset, len);
            if (trace) log.tracef("Split entry at %d:%d, allocated %d:%d, free %d:%d, %d free entries",
                  free.offset, free.size, newEntry.offset, newEntry.size, newFreeEntry.offset, newFreeEntry.size,
                  freeList.size());
            return newEntry;
         } catch (IOException e) {
            throw new PersistenceException("Cannot add new free entry", e);
         }
      }

      if (trace) log.tracef("Existing free entry allocated at %d:%d, %d free entries", free.offset, free.size, freeList.size());
      return free;
   }

   /**
    * Writes a new free entry to the file and also adds it to the free list
    */
   private void addNewFreeEntry(FileEntry fe) throws IOException {
      ByteBuffer buf = ByteBuffer.allocate(KEY_POS);
      buf.putInt(fe.size);
      buf.putInt(0);
      buf.putInt(0);
      buf.putInt(0);
      buf.putLong(-1);
      buf.flip();
      channel.write(buf, fe.offset);
      freeList.add(fe);
   }

   /**
    * Frees the space of the specified file entry (for reuse by allocate).
    * <p/>
    * Note: Caller must hold the {@code resizeLock} in shared mode.
    */
   private void free(FileEntry fe) throws IOException {
      if (fe != null) {
         // Invalidate entry on disk (by setting keyLen field to 0)
         // No need to wait for readers to unlock here, the FileEntry instance is not modified,
         // and allocate() won't return an entry as long as it has a reader.
         channel.write(ByteBuffer.wrap(ZERO_INT), fe.offset + KEYLEN_POS);
         if (!freeList.add(fe)) {
            throw new IllegalStateException(String.format("Trying to free an entry that was not allocated: %s", fe));
         }
         if (trace) log.tracef("Deleted entry at %d:%d, there are now %d free entries", fe.offset, fe.size, freeList.size());
      }
   }

   @Override
   public void write(MarshalledEntry<? extends K, ? extends V> marshalledEntry) {
      try {
         // serialize cache value
         org.infinispan.commons.io.ByteBuffer key = marshalledEntry.getKeyBytes();
         org.infinispan.commons.io.ByteBuffer data = marshalledEntry.getValueBytes();
         org.infinispan.commons.io.ByteBuffer metadata = marshalledEntry.getMetadataBytes();

         // allocate file entry and store in cache file
         int metadataLength = metadata == null ? 0 : metadata.getLength();
         int len = KEY_POS + key.getLength() + data.getLength() + metadataLength;
         FileEntry newEntry;
         FileEntry oldEntry = null;
         resizeLock.readLock().lock();
         try {
            newEntry = allocate(len);
            long expiryTime = metadata != null ? marshalledEntry.getMetadata().expiryTime() : -1;
            newEntry = new FileEntry(newEntry, key.getLength(), data.getLength(), metadataLength, expiryTime);

            ByteBuffer buf = ByteBuffer.allocate(len);
            buf.putInt(newEntry.size);
            buf.putInt(newEntry.keyLen);
            buf.putInt(newEntry.dataLen);
            buf.putInt(newEntry.metadataLen);
            buf.putLong(newEntry.expiryTime);
            buf.put(key.getBuf(), key.getOffset(), key.getLength());
            buf.put(data.getBuf(), data.getOffset(), data.getLength());
            if (metadata != null)
               buf.put(metadata.getBuf(), metadata.getOffset(), metadata.getLength());
            buf.flip();
            channel.write(buf, newEntry.offset);
            if (trace) log.tracef("Wrote entry %s:%d at %d:%d", marshalledEntry.getKey(), len, newEntry.offset, newEntry.size);

            // add the new entry to in-memory index
            oldEntry = entries.put(marshalledEntry.getKey(), newEntry);

            // if we added an entry, check if we need to evict something
            if (oldEntry == null)
               oldEntry = evict();
         } finally {
            // in case we replaced or evicted an entry, add to freeList
            try {
               free(oldEntry);
            } finally {
               resizeLock.readLock().unlock();
            }
         }
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   /**
    * Try to evict an entry if the capacity of the cache store is reached.
    *
    * @return FileEntry to evict, or null (if unbounded or capacity is not yet reached)
    */
   private FileEntry evict() {
      if (configuration.maxEntries() > 0) {
         synchronized (entries) {
            if (entries.size() > configuration.maxEntries()) {
               Iterator<FileEntry> it = entries.values().iterator();
               FileEntry fe = it.next();
               it.remove();
               return fe;
            }
         }
      }
      return null;
   }

   @Override
   public void clear() {
      resizeLock.writeLock().lock();
      try {
         synchronized (entries) {
            synchronized (freeList) {
               // wait until all readers are done reading file entries
               for (FileEntry fe : entries.values())
                  fe.waitUnlocked();
               for (FileEntry fe : freeList)
                  fe.waitUnlocked();

               // clear in-memory state
               entries.clear();
               freeList.clear();

               // reset file
               if (trace) log.tracef("Truncating file, current size is %d", filePos);
               channel.truncate(0);
               channel.write(ByteBuffer.wrap(MAGIC), 0);
               filePos = MAGIC.length;
            }
         }
      } catch (Exception e) {
         throw new PersistenceException(e);
      } finally {
         resizeLock.writeLock().unlock();
      }
   }

   @Override
   public boolean delete(Object key) {
      resizeLock.readLock().lock();
      try {
         FileEntry fe = entries.remove(key);
         free(fe);
         return fe != null;
      } catch (Exception e) {
         throw new PersistenceException(e);
      } finally {
         resizeLock.readLock().unlock();
      }
   }

   @Override
   public MarshalledEntry<K, V> load(Object key) {
      return _load(key, true, true);
   }

   private MarshalledEntry<K, V> _load(Object key, boolean loadValue, boolean loadMetadata) {
      final FileEntry fe;
      resizeLock.readLock().lock();
      try {
         synchronized (entries) {
            // lookup FileEntry of the key
            fe = entries.get(key);
            if (fe == null)
               return null;

            // Entries are removed due to expiration from {@link SingleFileStore#purge}
            if (fe.isExpired(timeService.wallClockTime())) {
               return null;
            } else {
               // lock entry for reading before releasing entries monitor
               fe.lock();
            }
         }
      } finally {
         resizeLock.readLock().unlock();
      }

      org.infinispan.commons.io.ByteBuffer valueBb = null;
      org.infinispan.commons.io.ByteBuffer metadataBb = null;

      // If we only require the key, then no need to read disk
      if (!loadValue && !loadMetadata) {
         try {
            return ctx.getMarshalledEntryFactory().newMarshalledEntry(key, valueBb, metadataBb);
         } finally {
            fe.unlock();
         }
      }

      final byte[] data;
      try {
         // load serialized data from disk
         data = new byte[fe.keyLen + fe.dataLen + (loadMetadata ? fe.metadataLen : 0)];
         // The entry lock will prevent clear() from truncating the file at this point
         channel.read(ByteBuffer.wrap(data), fe.offset + KEY_POS);
      } catch (Exception e) {
         throw new PersistenceException(e);
      } finally {
         // No need to keep the lock for deserialization.
         // FileEntry is immutable, so its members can't be changed by another thread.
         fe.unlock();
      }

      if (trace) log.tracef("Read entry %s at %d:%d", key, fe.offset, fe.actualSize());
      ByteBufferFactory factory = ctx.getByteBufferFactory();
      org.infinispan.commons.io.ByteBuffer keyBb = factory.newByteBuffer(data, 0, fe.keyLen);
      if (loadValue) {
         valueBb = factory.newByteBuffer(data, fe.keyLen, fe.dataLen);
      }
      if (loadMetadata && fe.metadataLen > 0) {
         metadataBb = factory.newByteBuffer(data, fe.keyLen + fe.dataLen, fe.metadataLen);
      }
      return ctx.getMarshalledEntryFactory().newMarshalledEntry(keyBb, valueBb, metadataBb);
   }

   @Override
   public Flowable<K> publishKeys(Predicate<? super K> filter) {
      return Flowable.fromIterable(() -> {
         List<K> keys = new ArrayList<>(entries.size());
         long now = ctx.getTimeService().wallClockTime();
         synchronized (entries) {
            for (Map.Entry<K, FileEntry> e : entries.entrySet()) {
               K key = e.getKey();
               if (!e.getValue().isExpired(now)  && (filter == null || filter.test(key))) {
                  keys.add(key);
               }
            }
         }
         // This way each invocation is a new copy
         return keys.iterator();
      });
   }

   @Override
   public Flowable<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      if (fetchMetadata || fetchValue) {
         return Flowable.fromIterable(() -> {
            // This way the sorting of entries is lazily done on each invocation of the publisher
            List<KeyValuePair<K, FileEntry>> keysToLoad = new ArrayList<>(entries.size());
            long now = ctx.getTimeService().wallClockTime();
            synchronized (entries) {
               for (Map.Entry<K, FileEntry> e : entries.entrySet()) {
                  if ((filter == null || filter.test(e.getKey())) && !e.getValue().isExpired(now)) {
                     keysToLoad.add(new KeyValuePair<>(e.getKey(), e.getValue()));
                  }
               }
            }

            keysToLoad.sort((o1, o2) -> {
               long offset1 = o1.getValue().offset;
               long offset2 = o2.getValue().offset;
               return offset1 < offset2 ? -1 : offset1 == offset2 ? 0 : 1;
            });
            return keysToLoad.iterator();
         }).map(kvp -> {
            MarshalledEntry<K, V> entry = _load(kvp.getKey(), fetchValue, fetchMetadata);
            if (entry == null) {
               // Rxjava2 doesn't allow nulls
               entry = MarshalledEntryImpl.empty();
            }
            return entry;
         }).filter(me -> me != MarshalledEntryImpl.empty());
      } else {
         return publishKeys(filter).map(k -> ctx.getMarshalledEntryFactory().newMarshalledEntry(k, (Object) null, null));
      }
   }

   /**
    * Manipulates the free entries for optimizing disk space.
    */
   private void processFreeEntries() {
      // Get a reverse sorted list of free entries based on file offset (bigger entries will be ahead of smaller entries)
      // This helps to work backwards with free entries at end of the file
      List<FileEntry> l  = new ArrayList<>(freeList);
      l.sort((o1, o2) -> {
         long diff = o1.offset - o2.offset;
         return (diff == 0) ? 0 : ((diff > 0) ? -1 : 1);
      });

      truncateFile(l);
      mergeFreeEntries(l);
   }

   /**
    * Removes free entries towards the end of the file and truncates the file.
    */
   private void truncateFile(List<FileEntry> entries) {
      long startTime = 0;
      if (trace) startTime = timeService.wallClockTime();

      int reclaimedSpace = 0;
      int removedEntries = 0;
      long truncateOffset = -1;
      for (Iterator<FileEntry> it = entries.iterator() ; it.hasNext(); ) {
         FileEntry fe = it.next();
         // Till we have free entries at the end of the file,
         // we can remove them and contract the file to release disk
         // space.
         if (!fe.isLocked() && ((fe.offset + fe.size) == filePos)) {
            truncateOffset = fe.offset;
            filePos = fe.offset;
            freeList.remove(fe);
            it.remove();
            reclaimedSpace += fe.size;
            removedEntries++;
         } else {
            break;
         }
      }

      if (truncateOffset > 0) {
         try {
            channel.truncate(truncateOffset);
         } catch (IOException e) {
            throw new PersistenceException("Error while truncating file", e);
         }
      }

      if (trace) {
         log.tracef("Removed entries: %d, Reclaimed Space: %d, Free Entries %d", removedEntries, reclaimedSpace, freeList.size());
         log.tracef("Time taken for truncateFile: %d (ms)", timeService.wallClockTime() - startTime);
      }
   }

   /**
    * Coalesces adjacent free entries to create larger free entries (so that the probability of finding a free entry during allocation increases)
    */
   private void mergeFreeEntries(List<FileEntry> entries) {
      long startTime = 0;
      if (trace) startTime = timeService.wallClockTime();
      FileEntry lastEntry = null;
      FileEntry newEntry = null;
      int mergeCounter = 0;
      for (FileEntry fe : entries) {
         if (fe.isLocked())
            continue;

         // Merge any holes created (consecutive free entries) in the file
         if ((lastEntry != null) && (lastEntry.offset == (fe.offset + fe.size))) {
            if (newEntry == null) {
               newEntry = new FileEntry(fe.offset, fe.size + lastEntry.size);
               freeList.remove(lastEntry);
               mergeCounter++;
            } else {
               newEntry = new FileEntry(fe.offset, fe.size + newEntry.size);
            }
            freeList.remove(fe);
            mergeCounter++;
         } else {
            if (newEntry != null) {
               mergeAndLogEntry(newEntry, mergeCounter);
               newEntry = null;
               mergeCounter = 0;
            }
         }
         lastEntry = fe;
      }

      if (newEntry != null)
         mergeAndLogEntry(newEntry, mergeCounter);

      if (trace) log.tracef("Total time taken for mergeFreeEntries: " + (timeService.wallClockTime() - startTime) + " (ms)");
   }

   private void mergeAndLogEntry(FileEntry entry, int mergeCounter) {
      try {
         addNewFreeEntry(entry);
         if (trace) log.tracef("Merged %d entries at %d:%d, %d free entries", mergeCounter, entry.offset, entry.size, freeList.size());
      } catch (IOException e) {
         throw new PersistenceException("Could not add new merged entry", e);
      }
   }
   @Override
   public void purge(Executor threadPool, final PurgeListener task) {
      long now = timeService.wallClockTime();
      List<KeyValuePair<Object, FileEntry>> entriesToPurge = new ArrayList<>();
      synchronized (entries) {
         for (Iterator<Map.Entry<K, FileEntry>> it = entries.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<K, FileEntry> next = it.next();
            FileEntry fe = next.getValue();
            if (fe.isExpired(now)) {
               it.remove();
               entriesToPurge.add(new KeyValuePair<>(next.getKey(), fe));
            }
         }
      }

      resizeLock.readLock().lock();
      try {
         for (Iterator<KeyValuePair<Object, FileEntry>> it = entriesToPurge.iterator(); it.hasNext(); ) {
            KeyValuePair<Object, FileEntry> next = it.next();
            FileEntry fe = next.getValue();
            if (fe.isExpired(now)) {
               it.remove();
               try {
                  free(fe);
               } catch (Exception e) {
                  throw new PersistenceException(e);
               }
               if (task != null) task.entryPurged(next.getKey());
            }
         }

         // Disk space optimizations
         synchronized (freeList) {
           processFreeEntries();
         }
      } finally {
         resizeLock.readLock().unlock();
      }
   }

   @Override
   public int size() {
      return entries.size();
   }

   Map<K, FileEntry> getEntries() {
      return entries;
   }

   SortedSet<FileEntry> getFreeList() {
      return freeList;
   }

   long getFileSize() {
      return filePos;
   }

   public SingleFileStoreConfiguration getConfiguration() {
      return configuration;
   }

   /**
    * Helper class to represent an entry in the cache file.
    * <p/>
    * The format of a FileEntry on disk is as follows:
    * <ul>
    * <li>4 bytes: {@link #size}</li>
    * <li>4 bytes: {@link #keyLen}, 0 if the block is unused</li>
    * <li>4 bytes: {@link #dataLen}</li>
    * <li>4 bytes: {@link #metadataLen}</li>
    * <li>8 bytes: {@link #expiryTime}</li>
    * <li>{@link #keyLen} bytes: serialized key</li>
    * <li>{@link #dataLen} bytes: serialized data</li>
    * <li>{@link #metadataLen} bytes: serialized key</li>
    * </ul>
    */
   private static class FileEntry implements Comparable<FileEntry> {
      /**
       * File offset of this block.
       */
      final long offset;

      /**
       * Total size of this block.
       */
      final int size;

      /**
       * Size of serialized key.
       */
      final int keyLen;

      /**
       * Size of serialized data.
       */
      final int dataLen;

      /**
       * Size of serialized metadata.
       */
      final int metadataLen;

      /**
       * Time stamp when the entry will expire (i.e. will be collected by purge).
       */
      final long expiryTime;

      /**
       * Number of current readers.
       */
      transient int readers = 0;

      FileEntry(long offset, int size) {
         this(offset, size, 0, 0, 0, -1);
      }

      FileEntry(long offset, int size, int keyLen, int dataLen, int metadataLen, long expiryTime) {
         this.offset = offset;
         this.size = size;
         this.keyLen = keyLen;
         this.dataLen = dataLen;
         this.metadataLen = metadataLen;
         this.expiryTime = expiryTime;
      }

      FileEntry(FileEntry fe, int keyLen, int dataLen, int metadataLen, long expiryTime) {
         this(fe.offset, fe.size, keyLen, dataLen, metadataLen, expiryTime);
      }

      synchronized boolean isLocked() {
         return readers > 0;
      }

      synchronized void lock() {
         readers++;
      }

      synchronized void unlock() {
         readers--;
         if (readers == 0)
            notifyAll();
      }

      synchronized void waitUnlocked() {
         while (readers > 0) {
            try {
               wait();
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
         }
      }

      boolean isExpired(long now) {
         return expiryTime > 0 && expiryTime < now;
      }

      int actualSize() {
         return KEY_POS + keyLen + dataLen + metadataLen;
      }

      @Override
      public int compareTo(FileEntry fe) {
         // We compare the size first, as the entries in the free list must be sorted by size
         int diff = size - fe.size;
         if (diff != 0) return diff;
         return (offset < fe.offset) ? -1 : ((offset == fe.offset) ? 0 : 1);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         FileEntry fileEntry = (FileEntry) o;

         if (offset != fileEntry.offset) return false;
         if (size != fileEntry.size) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = (int) (offset ^ (offset >>> 32));
         result = 31 * result + size;
         return result;
      }

      @Override
      public String toString() {
         return "FileEntry@" +
               offset +
               "{size=" + size +
               ", actual=" + actualSize() +
               '}';
      }
   }
}
