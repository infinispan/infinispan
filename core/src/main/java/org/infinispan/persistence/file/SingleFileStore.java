package org.infinispan.persistence.file;

import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ByRef;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Flowable;

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

   public static final byte[] MAGIC_BEFORE_11 = new byte[]{'F', 'C', 'S', '1'}; //<11
   public static final byte[] MAGIC_11_0 = new byte[]{'F', 'C', 'S', '2'};
   public static final byte[] MAGIC_12_0 = new byte[]{'F', 'C', 'S', '3'};
   public static final byte[] MAGIC_LATEST = MAGIC_12_0;
   private static final byte[] ZERO_INT = {0, 0, 0, 0};
   private static final int KEYLEN_POS = 4;
   /*
    * 4 bytes - entry size
    * 4 bytes - key length
    * 4 bytes - value length
    * 4 bytes - metadata length
    * 8 bytes - expiration time
    */
   public static final int KEY_POS_BEFORE_11 = 4 + 4 + 4 + 4 + 8;
   /*
    * 4 bytes - entry size
    * 4 bytes - key length
    * 4 bytes - value length
    * 4 bytes - metadata length
    * 4 bytes - internal metadata length
    * 8 bytes - expiration time
    */
   public static final int KEY_POS_11_0 = 4 + 4 + 4 + 4 + 4 + 8;
   public static final int KEY_POS_LATEST = KEY_POS_11_0;

   // bytes required by created and lastUsed timestamps
   private static final int TIMESTAMP_BYTES = 8 + 8;
   private static final int SMALLEST_ENTRY_SIZE = 128;

   private SingleFileStoreConfiguration configuration;

   protected InitializationContext ctx;

   private FileChannel channel;
   private Map<K, FileEntry> entries;
   private SortedSet<FileEntry> freeList;
   private long filePos = MAGIC_LATEST.length;
   private File file;
   private float fragmentationFactor = .75f;
   // Prevent clear() from truncating the file after a write() allocated the entry but before it wrote the data
   private final ReadWriteLock resizeLock = new ReentrantReadWriteLock();
   private TimeService timeService;
   private MarshallableEntryFactory<K, V> entryFactory;

   public static File getStoreFile(GlobalConfiguration globalConfiguration, String locationPath, String cacheName) {
      Path location = PersistenceUtil.getLocation(globalConfiguration, locationPath);
      return new File(location.toFile(), cacheName + ".dat");
   }

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
      this.timeService = ctx.getTimeService();
      this.entryFactory = ctx.getMarshallableEntryFactory();
   }

   @Override
   public void start() {
      try {
         file = getStoreFile(ctx.getGlobalConfiguration(), configuration.location(), ctx.getCache().getName());
         if (!file.exists()) {
            File dir = file.getParentFile();
            if (!dir.mkdirs() && !dir.exists()) {
               throw PERSISTENCE.directoryCannotBeCreated(dir.getAbsolutePath());
            }
         }
         channel = new RandomAccessFile(file, "rw").getChannel();

         // initialize data structures. Only use LinkedHashMap (LRU) for entries when cache store is bounded
         Map<K, FileEntry> entryMap = configuration.maxEntries() > 0 ?
               new LinkedHashMap<>(16, 0.75f, true) :
               new HashMap<>();
         entries = Collections.synchronizedMap(entryMap);
         freeList = Collections.synchronizedSortedSet(new TreeSet<>());

         // check file format and read persistent state if enabled for the cache
         byte[] header = new byte[MAGIC_LATEST.length];
         if (!configuration.purgeOnStartup() && channel.read(ByteBuffer.wrap(header), 0) == MAGIC_LATEST.length) {
            if (Arrays.equals(MAGIC_LATEST, header)) {
               rebuildIndex();
               processFreeEntries();
            } else if (Arrays.equals(MAGIC_11_0, header)) {
               migrateFromV11();
               processFreeEntries();
            } else if (Arrays.equals(MAGIC_BEFORE_11, header)) {
               throw PERSISTENCE.persistedDataMigrationAcrossMajorVersions();
            } else {
               clear(); // otherwise (unknown file format or no preload) just reset the file
            }
         }
         else
            clear(); // otherwise (unknown file format or no preload) just reset the file

         // Initialize the fragmentation factor
         fragmentationFactor = configuration.fragmentationFactor();
      } catch (PersistenceException e) {
         throw e;
      } catch (Throwable t) {
         throw new PersistenceException(t);
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
            filePos = MAGIC_LATEST.length;
         }
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   @Override
   public void destroy() {
      stop();
      if (file.delete()) {
         log.tracef("Deleted file: " + file);
      } else {
         log.tracef("Could not delete file: " + file);
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
      ByteBuffer buf = ByteBuffer.allocate(KEY_POS_LATEST);
      for (; ; ) {
         // read FileEntry fields from file (size, keyLen etc.)
         buf = readChannel(buf, filePos, KEY_POS_11_0);
         // return if end of file is reached
         if (buf.remaining() > 0)
            break;
         buf.flip();

         // initialize FileEntry from buffer
         FileEntry fe = new FileEntry(filePos, buf);

         // sanity check
         if (fe.size < KEY_POS_LATEST + fe.keyLen + fe.dataLen + fe.metadataLen + fe.internalMetadataLen) {
            throw PERSISTENCE.errorReadingFileStore(file.getPath(), filePos);
         }

         // update file pointer
         filePos += fe.size;

         // check if the entry is used or free
         if (fe.keyLen > 0) {
            // load the key from file
            buf = readChannel(buf, fe.offset + KEY_POS_LATEST, fe.keyLen);

            // deserialize key and add to entries map
            // Marshaller should allow for provided type return for safety
            K key = (K) ctx.getPersistenceMarshaller().objectFromByteBuffer(buf.array(), 0, fe.keyLen);
            entries.put(key, fe);
         } else {
            // add to free list
            freeList.add(fe);
         }
      }
   }

   private void migrateFromV11() {
      String cacheName = ctx.getCache().getName();
      PERSISTENCE.startMigratingPersistenceData(cacheName);
      File newFile = new File(file.getParentFile(), cacheName + "_new.dat");
      if (newFile.exists()) {
         newFile.delete();
      }
      long newFilePos = MAGIC_LATEST.length;
      long oldFilePos = MAGIC_11_0.length;
      // Only update the key/value/meta bytes if the default marshaller is configured
      boolean transformationRequired = ctx.getGlobalConfiguration().serialization().marshaller() == null;

      try (FileChannel newChannel = new RandomAccessFile(newFile, "rw").getChannel()) {
         //Write Magic
         newChannel.truncate(0);
         newChannel.write(ByteBuffer.wrap(MAGIC_LATEST), 0);

         ByteBuffer buf = ByteBuffer.allocate(KEY_POS_LATEST);
         for (; ; ) {
            // read FileEntry fields from file (size, keyLen etc.)
            buf = readChannelUpdateOffset(buf, new ByRef.Long(oldFilePos), KEY_POS_11_0);
            if (buf.remaining() > 0)
               break;

            buf.flip();

            // initialize FileEntry from buffer
            FileEntry oldFe = new FileEntry(oldFilePos, buf);

            // sanity check
            if (oldFe.size < KEY_POS_11_0 + oldFe.keyLen + oldFe.dataLen + oldFe.metadataLen + oldFe.internalMetadataLen) {
               throw PERSISTENCE.errorReadingFileStore(file.getPath(), filePos);
            }

            //update old file pos to the next entry
            oldFilePos += oldFe.size;

            // check if the entry is used or free
            // if it is free, it is ignored.
            if (oldFe.keyLen < 1)
               continue;

            int remainingLength;
            ByRef.Long offset = new ByRef.Long(oldFe.offset + KEY_POS_11_0);
            K key = loadLegacyObject(buf, offset, oldFe.keyLen, false);
            if (transformationRequired) {
               ByteBuffer newKey = transformLegacyObject(key);
               ByteBuffer newValue = loadAndTransformLegacy(buf, offset, oldFe.dataLen, false);

               ByteBuffer newMeta = null;
               if (oldFe.metadataLen > 0) {
                  newMeta = loadAndTransformLegacy(buf, offset, oldFe.metadataLen - TIMESTAMP_BYTES, true);
               }

               int newMetaSize = newMeta != null ? newMeta.limit() + TIMESTAMP_BYTES : 0;
               FileEntry newFe = new FileEntry(newFilePos, oldFe.size, newKey.limit(), newValue.limit(), newMetaSize,
                     oldFe.internalMetadataLen, oldFe.expiryTime);

               // write the FileEntry to new file
               buf = allocate(buf, KEY_POS_LATEST);
               newFe.writeToBuf(buf);
               buf.flip();
               newChannel.write(buf, newFilePos);
               newFilePos += KEY_POS_LATEST; //size written
               entries.put(key, newFe);

               //write the updated content to the new file
               newFilePos += newChannel.write(newKey, newFilePos);
               newFilePos += newChannel.write(newValue, newFilePos);
               if (newMeta != null) {
                  newFilePos += newChannel.write(newMeta, newFilePos);
               }
               newChannel.write(newKey, newFilePos);
               newFilePos += newFe.keyLen;
               newChannel.write(newValue, newFilePos);
               newFilePos += newFe.dataLen;
               if (newMeta != null) {
                  newChannel.write(newMeta, newFilePos);
                  newFilePos += newFe.metadataLen;
               }

               remainingLength = (newFe.expiryTime > 0 ? TIMESTAMP_BYTES : 0) + oldFe.internalMetadataLen + 8;
            } else {
               // Simply use the old FileEntry as only the magic bytes are updated
               entries.put(key, oldFe);
               remainingLength = oldFe.size - oldFe.keyLen - KEY_POS_11_0;
            }

            // Read and write the remaining data from old file
            buf = readChannelUpdateOffset(buf, offset, remainingLength);
            newFilePos += newChannel.write(buf, newFilePos);
         }
      } catch (IOException | ClassNotFoundException e) {
         throw PERSISTENCE.persistedDataMigrationFailed(cacheName, e);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw PERSISTENCE.persistedDataMigrationFailed(cacheName, e);
      }

      try {
         //close old file
         channel.close();
         //replace old file with the new file
         Files.move(newFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
         //reopen the file
         channel = new RandomAccessFile(file, "rw").getChannel();
         //update file position
         filePos = newFilePos;
         PERSISTENCE.persistedDataSuccessfulMigrated(cacheName);
      } catch (IOException e) {
         throw PERSISTENCE.persistedDataMigrationFailed(cacheName, e);
      }
   }

   private ByteBuffer loadAndTransformLegacy(ByteBuffer buf, ByRef.Long offset, int length, boolean allowInternal)
         throws ClassNotFoundException, InterruptedException, IOException {
      Object obj = loadLegacyObject(buf, offset, length, allowInternal);
      return transformLegacyObject(obj);
   }

   private <T> ByteBuffer transformLegacyObject(T object) throws InterruptedException, IOException {
      byte[] bytes = ctx.getPersistenceMarshaller().objectToByteBuffer(object);
      return ByteBuffer.wrap(bytes);
   }

   @SuppressWarnings("unchecked")
   private <T> T loadLegacyObject(ByteBuffer buf, ByRef.Long offset, int length, boolean allowInternal) throws ClassNotFoundException, IOException {
      buf = readChannelUpdateOffset(buf, offset, length);
      // Read using raw user marshaller without MarshallUserObject wrapping
      byte[] bytes = buf.array();
      Marshaller marshaller = ctx.getPersistenceMarshaller().getUserMarshaller();
      try {
         return (T) marshaller.objectFromByteBuffer(bytes, 0, length);
      } catch (IllegalArgumentException e) {
         // For metadata we need to attempt to read with user-marshaller first in case custom metadata used, otherwise use the persistence marshaller
         if (allowInternal) {
            return (T) ctx.getPersistenceMarshaller().objectFromByteBuffer(bytes, 0, length);
         }
         throw e;
      }
   }

   private ByteBuffer readChannelUpdateOffset(ByteBuffer buf, ByRef.Long offset, int length) throws IOException {
      return readChannel(buf, offset.getAndAdd(length), length);
   }

   private ByteBuffer readChannel(ByteBuffer buf, long offset, int length) throws IOException {
      buf = allocate(buf, length);
      channel.read(buf, offset);
      return buf;
   }

   private ByteBuffer allocate(ByteBuffer buf, int length) {
      buf.flip();
      if (buf.capacity() < length) {
         buf = ByteBuffer.allocate(length);
      }
      buf.clear().limit(length);
      return buf;
   }

   /**
    * The base class implementation calls {@link CacheLoader#loadEntry(Object)} for this, we can do better because
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
         if (log.isTraceEnabled()) log.tracef("New entry allocated at %d:%d, %d free entries, file size is %d", fe.offset, fe.size, freeList.size(), filePos);
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
            if (log.isTraceEnabled()) log.tracef("Split entry at %d:%d, allocated %d:%d, free %d:%d, %d free entries",
                  free.offset, free.size, newEntry.offset, newEntry.size, newFreeEntry.offset, newFreeEntry.size,
                  freeList.size());
            return newEntry;
         } catch (IOException e) {
            throw new PersistenceException("Cannot add new free entry", e);
         }
      }

      if (log.isTraceEnabled()) log.tracef("Existing free entry allocated at %d:%d, %d free entries", free.offset, free.size, freeList.size());
      return free;
   }

   /**
    * Writes a new free entry to the file and also adds it to the free list
    */
   private void addNewFreeEntry(FileEntry fe) throws IOException {
      ByteBuffer buf = ByteBuffer.allocate(KEY_POS_LATEST);
      buf.putInt(fe.size);
      buf.putInt(0);
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
         if (log.isTraceEnabled()) log.tracef("Deleted entry at %d:%d, there are now %d free entries", fe.offset, fe.size, freeList.size());
      }
   }

   @Override
   public void write(MarshallableEntry<? extends K, ? extends V> marshalledEntry) {
      try {
         // serialize cache value
         org.infinispan.commons.io.ByteBuffer key = marshalledEntry.getKeyBytes();
         org.infinispan.commons.io.ByteBuffer data = marshalledEntry.getValueBytes();
         org.infinispan.commons.io.ByteBuffer metadata = marshalledEntry.getMetadataBytes();
         org.infinispan.commons.io.ByteBuffer internalMetadata = marshalledEntry.getInternalMetadataBytes();

         // allocate file entry and store in cache file
         int metadataLength = metadata == null ? 0 : metadata.getLength() + TIMESTAMP_BYTES;
         int internalMetadataLength = internalMetadata == null ? 0 : internalMetadata.getLength();
         int len = KEY_POS_LATEST + key.getLength() + data.getLength() + metadataLength + internalMetadataLength;
         FileEntry newEntry;
         FileEntry oldEntry = null;
         resizeLock.readLock().lock();
         try {
            newEntry = allocate(len);
            newEntry = new FileEntry(newEntry.offset, newEntry.size, key.getLength(), data.getLength(), metadataLength, internalMetadataLength, marshalledEntry.expiryTime());

            ByteBuffer buf = ByteBuffer.allocate(len);
            newEntry.writeToBuf(buf);
            buf.put(key.getBuf(), key.getOffset(), key.getLength());
            buf.put(data.getBuf(), data.getOffset(), data.getLength());
            if (metadata != null) {
               buf.put(metadata.getBuf(), metadata.getOffset(), metadata.getLength());

               // Only write created & lastUsed if expiryTime is set
               if (newEntry.expiryTime > 0) {
                  buf.putLong(marshalledEntry.created());
                  buf.putLong(marshalledEntry.lastUsed());
               }
            }
            if (internalMetadata != null) {
               buf.put(internalMetadata.getBuf(), internalMetadata.getOffset(), internalMetadata.getLength());
            }
            buf.flip();
            channel.write(buf, newEntry.offset);
            if (log.isTraceEnabled()) log.tracef("Wrote entry %s:%d at %d:%d", marshalledEntry.getKey(), len, newEntry.offset, newEntry.size);

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
               if (log.isTraceEnabled()) log.tracef("Truncating file, current size is %d", filePos);
               channel.truncate(0);
               channel.write(ByteBuffer.wrap(MAGIC_LATEST), 0);
               filePos = MAGIC_LATEST.length;
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
   public MarshallableEntry<K, V> loadEntry(Object key) {
      return _load(key, true, true);
   }

   private MarshallableEntry<K, V> _load(Object key, boolean loadValue, boolean loadMetadata) {
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

      // If we only require the key, then no need to read disk
      if (!loadValue && !loadMetadata) {
         try {
            return entryFactory.create(key);
         } finally {
            fe.unlock();
         }
      }

      final byte[] data;
      try {
         // load serialized data from disk
         data = new byte[fe.keyLen + fe.dataLen + (loadMetadata ? fe.metadataLen + fe.internalMetadataLen : 0)];
         // The entry lock will prevent clear() from truncating the file at this point
         channel.read(ByteBuffer.wrap(data), fe.offset + KEY_POS_LATEST);
      } catch (Exception e) {
         throw new PersistenceException(e);
      } finally {
         // No need to keep the lock for deserialization.
         // FileEntry is immutable, so its members can't be changed by another thread.
         fe.unlock();
      }

      if (log.isTraceEnabled()) log.tracef("Read entry %s at %d:%d", key, fe.offset, fe.actualSize());
      ByteBufferFactory factory = ctx.getByteBufferFactory();
      org.infinispan.commons.io.ByteBuffer keyBb = factory.newByteBuffer(data, 0, fe.keyLen);

      if (loadValue) {
         valueBb = factory.newByteBuffer(data, fe.keyLen, fe.dataLen);
      }
      if (loadMetadata) {
         long created = -1;
         long lastUsed = -1;
         org.infinispan.commons.io.ByteBuffer metadataBb = null;
         org.infinispan.commons.io.ByteBuffer internalMetadataBb = null;

         int offset = fe.keyLen + fe.dataLen;
         if (fe.metadataLen > 0) {
            int metaLength = fe.metadataLen - TIMESTAMP_BYTES;
            metadataBb = factory.newByteBuffer(data, offset, metaLength);

            offset += metaLength;

            ByteBuffer buffer = ByteBuffer.wrap(data, offset, TIMESTAMP_BYTES);
            if (fe.expiryTime > 0) {
               offset += TIMESTAMP_BYTES;
               created = buffer.getLong();
               lastUsed = buffer.getLong();
            }
         }

         if (fe.internalMetadataLen > 0) {
            internalMetadataBb = factory.newByteBuffer(data, offset, fe.internalMetadataLen);
         }

         return entryFactory.create(keyBb, valueBb, metadataBb, internalMetadataBb, created, lastUsed);
      }
      return entryFactory.create(keyBb, valueBb);
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
   public Flowable<MarshallableEntry<K, V>> entryPublisher(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
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

            keysToLoad.sort(Comparator.comparingLong(o -> o.getValue().offset));
            return keysToLoad.iterator();
         }).map(kvp -> {
            MarshallableEntry<K, V> entry = _load(kvp.getKey(), fetchValue, fetchMetadata);
            if (entry == null) {
               // Rxjava2 doesn't allow nulls
               entry = entryFactory.getEmpty();
            }
            return entry;
         }).filter(me -> me != entryFactory.getEmpty());
      } else {
         return publishKeys(filter).map(k -> entryFactory.create(k));
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
      if (log.isTraceEnabled()) startTime = timeService.wallClockTime();

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

      if (log.isTraceEnabled()) {
         log.tracef("Removed entries: %d, Reclaimed Space: %d, Free Entries %d", removedEntries, reclaimedSpace, freeList.size());
         log.tracef("Time taken for truncateFile: %d (ms)", timeService.wallClockTime() - startTime);
      }
   }

   /**
    * Coalesces adjacent free entries to create larger free entries (so that the probability of finding a free entry during allocation increases)
    */
   private void mergeFreeEntries(List<FileEntry> entries) {
      long startTime = 0;
      if (log.isTraceEnabled()) startTime = timeService.wallClockTime();
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

      if (log.isTraceEnabled()) log.tracef("Total time taken for mergeFreeEntries: " + (timeService.wallClockTime() - startTime) + " (ms)");
   }

   private void mergeAndLogEntry(FileEntry entry, int mergeCounter) {
      try {
         addNewFreeEntry(entry);
         if (log.isTraceEnabled()) log.tracef("Merged %d entries at %d:%d, %d free entries", mergeCounter, entry.offset, entry.size, freeList.size());
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
       * Size of serialized internal metadata.
       */
      final int internalMetadataLen;

      /**
       * Time stamp when the entry will expire (i.e. will be collected by purge).
       */
      final long expiryTime;

      /**
       * Number of current readers.
       */
      transient int readers = 0;

      FileEntry(long offset, ByteBuffer buf) {
         this.offset = offset;
         this.size = buf.getInt();
         this.keyLen = buf.getInt();
         this.dataLen = buf.getInt();
         this.metadataLen = buf.getInt();
         this.internalMetadataLen = buf.getInt();
         this.expiryTime = buf.getLong();
      }

      FileEntry(long offset, int size) {
         this(offset, size, 0, 0, 0, 0, -1);
      }

      FileEntry(long offset, int size, int keyLen, int dataLen, int metadataLen, int internalMetadataLen, long expiryTime) {
         this.offset = offset;
         this.size = size;
         this.keyLen = keyLen;
         this.dataLen = dataLen;
         this.metadataLen = metadataLen;
         this.internalMetadataLen = internalMetadataLen;
         this.expiryTime = expiryTime;
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
         return KEY_POS_LATEST + keyLen + dataLen + metadataLen + internalMetadataLen;
      }

      void writeToBuf(ByteBuffer buf) {
         buf.putInt(size);
         buf.putInt(keyLen);
         buf.putInt(dataLen);
         buf.putInt(metadataLen);
         buf.putInt(internalMetadataLen);
         buf.putLong(expiryTime);
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
