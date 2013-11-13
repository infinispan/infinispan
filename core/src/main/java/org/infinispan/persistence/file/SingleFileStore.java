package org.infinispan.persistence.file;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.equivalence.EquivalentLinkedHashMap;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.executors.ExecutorAllCompletionService;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A filesystem-based implementation of a {@link org.infinispan.persistence.spi.CacheLoader}. This file store
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
public class SingleFileStore implements AdvancedLoadWriteStore {
   private static final Log log = LogFactory.getLog(SingleFileStore.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final byte[] MAGIC = new byte[]{'F', 'C', 'S', '1'};
   private static final byte[] ZERO_INT = {0, 0, 0, 0};
   private static final int KEYLEN_POS = 4;
   private static final int KEY_POS = 4 + 4 + 4 + 4 + 8;

   private SingleFileStoreConfiguration configuration;

   protected InitializationContext ctx;

   private FileChannel channel;
   private Map<Object, FileEntry> entries;
   private SortedSet<FileEntry> freeList;
   private long filePos = MAGIC.length;
   private File file;
   // Prevent clear() from truncating the file after a write() allocated the entry but before it wrote the data
   private ReadWriteLock resizeLock = new ReentrantReadWriteLock();

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
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

         // initialize data structures
         entries = newEntryMap();
         freeList = Collections.synchronizedSortedSet(new TreeSet<FileEntry>());

         // check file format and read persistent state if enabled for the cache
         byte[] header = new byte[MAGIC.length];
         if (channel.read(ByteBuffer.wrap(header), 0) == MAGIC.length && Arrays.equals(MAGIC, header))
            rebuildIndex();
         else
            clear(); // otherwise (unknown file format or no preload) just reset the file
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   private Map<Object, FileEntry> newEntryMap() {
      // only use LinkedHashMap (LRU) for entries when cache store is bounded
      final Map<Object, FileEntry> entryMap;
      Equivalence<Object> keyEq = ctx.getCache().getCacheConfiguration().dataContainer().keyEquivalence();
      if (configuration.maxEntries() > 0)
         entryMap = CollectionFactory.makeLinkedMap(16, 0.75f,
               EquivalentLinkedHashMap.IterationOrder.ACCESS_ORDER,
               keyEq, AnyEquivalence.<FileEntry>getInstance());
      else
         entryMap = CollectionFactory.makeMap(keyEq, AnyEquivalence.<FileEntry>getInstance());

      return Collections.synchronizedMap(entryMap);
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
            Object key = ctx.getMarshaller().objectFromByteBuffer(buf.array(), 0, fe.keyLen);
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
      return entries.containsKey(key);
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
            return free;
         }

         // no appropriate free section available, append at end of file
         FileEntry fe = new FileEntry(filePos, len);
         filePos += len;
         log.tracef("New entry allocated, file size is %d", filePos);
         return fe;
      }
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
         if (trace) log.tracef("Deleted entry at %d", fe.offset);
         freeList.add(fe);
      }
   }

   @Override
   public void write(MarshalledEntry marshalledEntry) {
      try {
         // serialize cache value
         org.infinispan.commons.io.ByteBuffer key = marshalledEntry.getKeyBytes();
         org.infinispan.commons.io.ByteBuffer data = marshalledEntry.getValueBytes();
         org.infinispan.commons.io.ByteBuffer metadata = marshalledEntry.getMetadataBytes();

         // allocate file entry and store in cache file
         int metadataLength = metadata == null ? 0 : metadata.getLength();
         int len = KEY_POS + key.getLength() + data.getLength() + metadataLength;
         FileEntry fe = null;
         resizeLock.readLock().lock();
         try {
            fe = allocate(len);
            long expiryTime = metadata != null ? marshalledEntry.getMetadata().expiryTime() : -1;
            fe = new FileEntry(fe, key.getLength(), data.getLength(), metadataLength, expiryTime);

            ByteBuffer buf = ByteBuffer.allocate(len);
            buf.putInt(fe.size);
            buf.putInt(fe.keyLen);
            buf.putInt(fe.dataLen);
            buf.putInt(fe.metadataLen);
            buf.putLong(fe.expiryTime);
            buf.put(key.getBuf(), key.getOffset(), key.getLength());
            buf.put(data.getBuf(), data.getOffset(), data.getLength());
            if (metadata != null)
               buf.put(metadata.getBuf(), metadata.getOffset(), metadata.getLength());
            buf.flip();
            channel.write(buf, fe.offset);
            if (trace) log.tracef("Wrote entry %s at %d", marshalledEntry.getKey(), fe.offset);

            // add the new entry to in-memory index
            fe = entries.put(marshalledEntry.getKey(), fe);

            // if we added an entry, check if we need to evict something
            if (fe == null)
               fe = evict();
         } finally {
            // in case we replaced or evicted an entry, add to freeList
            try {
               free(fe);
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
   public MarshalledEntry load(Object key) {
      return _load(key, true, true);
   }

   private MarshalledEntry _load(Object key, boolean loadValue, boolean loadMetadata) {
      final FileEntry fe;
      final boolean expired;
      resizeLock.readLock().lock();
      try {
         synchronized (entries) {
            // lookup FileEntry of the key
            fe = entries.get(key);
            if (fe == null)
               return null;

            expired = fe.isExpired(System.currentTimeMillis());
            if (expired) {
               // if expired, remove the entry (within entries monitor)
               entries.remove(key);
            } else {
               // lock entry for reading before releasing entries monitor
               fe.lock();
            }
         }

         if (expired) {
            // if expired, free the file entry (after releasing entries monitor)
            try {
               free(fe);
            } catch (IOException e) {
               throw new PersistenceException(e);
            }
            return null;
         }
      } finally {
         resizeLock.readLock().unlock();
      }

      final byte[] data;
      try {
         // load serialized data from disk
         data = new byte[fe.keyLen + (loadValue ? fe.dataLen : 0) + (loadMetadata ? fe.metadataLen : 0)];
         // The entry lock will prevent clear() from truncating the file at this point
         channel.read(ByteBuffer.wrap(data), fe.offset + KEY_POS);
      } catch (Exception e) {
         throw new PersistenceException(e);
      } finally {
         // No need to keep the lock for deserialization.
         // FileEntry is immutable, so its members can't be changed by another thread.
         fe.unlock();
      }

      if (trace) log.tracef("Read entry %s at %d", key, fe.offset);
      ByteBufferFactory factory = ctx.getByteBufferFactory();
      org.infinispan.commons.io.ByteBuffer keyBb = factory.newByteBuffer(data, 0, fe.keyLen);
      org.infinispan.commons.io.ByteBuffer valueBb = null;
      org.infinispan.commons.io.ByteBuffer metadataBb = null;
      if (loadValue) {
         valueBb = factory.newByteBuffer(data, fe.keyLen, fe.dataLen);
         if (loadMetadata && fe.metadataLen > 0)
            metadataBb = factory.newByteBuffer(data, fe.keyLen + fe.dataLen, fe.metadataLen);
      }
      return ctx.getMarshalledEntryFactory().newMarshalledEntry(keyBb, valueBb, metadataBb);
   }

   @Override
   public void process(KeyFilter filter, final CacheLoaderTask task, Executor executor, final boolean fetchValue, final boolean fetchMetadata) {
      filter = PersistenceUtil.notNull(filter);
      Set<Object> keysToLoad = new HashSet<Object>(entries.size());
      synchronized (entries) {
         for (Object k : entries.keySet()) {
            if (filter.shouldLoadKey(k))
               keysToLoad.add(k);
         }
      }

      ExecutorAllCompletionService eacs = new ExecutorAllCompletionService(executor);

      final TaskContextImpl taskContext = new TaskContextImpl();
      int taskCount = 0;
      for (final Object key : keysToLoad) {
         if (taskContext.isStopped())
            break;

         eacs.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
               try {
                  final MarshalledEntry marshalledEntry = _load(key, fetchValue, fetchMetadata);
                  if (marshalledEntry != null) {
                     task.processEntry(marshalledEntry, taskContext);
                  }
                  return null;
               } catch (Exception e) {
                  log.errorExecutingParallelStoreTask(e);
                  throw e;
               }
            }
         });
      }
      eacs.waitUntilAllCompleted();
      if (eacs.isExceptionThrown()) {
         throw new PersistenceException("Execution exception!", eacs.getFirstException());
      }
   }

   @Override
   public void purge(Executor threadPool, final PurgeListener task) {

      threadPool.execute(new Runnable() {
         @Override
         public void run() {
            long now = System.currentTimeMillis();
            List<KeyValuePair<Object, FileEntry>> entriesToPurge = new ArrayList<KeyValuePair<Object, FileEntry>>();
            synchronized (entries) {
               for (Iterator<Map.Entry<Object, FileEntry>> it = entries.entrySet().iterator(); it.hasNext(); ) {
                  Map.Entry<Object, FileEntry> next = it.next();
                  FileEntry fe = next.getValue();
                  if (fe.isExpired(now)) {
                     it.remove();
                     entriesToPurge.add(new KeyValuePair<Object, FileEntry>(next.getKey(), fe));
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
            } finally {
               resizeLock.readLock().unlock();
            }
         }
      });
   }

   @Override
   public int size() {
      return entries.size();
   }

   Map<Object, FileEntry> getEntries() {
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
      private final long offset;

      /**
       * Total size of this block.
       */
      private final int size;

      /**
       * Size of serialized key.
       */
      private final int keyLen;

      /**
       * Size of serialized data.
       */
      private final int dataLen;

      /**
       * Size of serialized metadata.
       */
      private final int metadataLen;

      /**
       * Time stamp when the entry will expire (i.e. will be collected by purge).
       */
      private final long expiryTime;

      /**
       * Number of current readers.
       */
      private transient int readers = 0;

      public FileEntry(long offset, int size) {
         this(offset, size, 0, 0, 0, -1);
      }

      public FileEntry(long offset, int size, int keyLen, int dataLen, int metadataLen, long expiryTime) {
         this.offset = offset;
         this.size = size;
         this.keyLen = keyLen;
         this.dataLen = dataLen;
         this.metadataLen = metadataLen;
         this.expiryTime = expiryTime;
      }

      public FileEntry(FileEntry fe, int keyLen, int dataLen, int metadataLen, long expiryTime) {
         this(fe.offset, fe.size, keyLen, dataLen, metadataLen, expiryTime);
      }

      public synchronized boolean isLocked() {
         return readers > 0;
      }

      public synchronized void lock() {
         readers++;
      }

      public synchronized void unlock() {
         readers--;
         if (readers == 0)
            notifyAll();
      }

      public synchronized void waitUnlocked() {
         while (readers > 0) {
            try {
               wait();
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
         }
      }

      public boolean isExpired(long now) {
         return expiryTime > 0 && expiryTime < now;
      }

      @Override
      public int compareTo(FileEntry fe) {
         int diff = size - fe.size;
         return (diff != 0) ? diff : offset > fe.offset ? 1 : -1;
      }
   }
}
