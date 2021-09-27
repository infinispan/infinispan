package org.infinispan.persistence.file;

import static io.reactivex.rxjava3.core.Flowable.defer;
import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.AbstractSegmentedStoreConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.TopologyIracVersion;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import net.jcip.annotations.GuardedBy;

/**
 * A filesystem-based implementation of a {@link org.infinispan.persistence.spi.NonBlockingStore}.
 * This file store stores cache values in a single file <tt>&lt;location&gt;/&lt;cache name&gt;.dat</tt>,
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
@ConfiguredBy(SingleFileStoreConfiguration.class)
public class SingleFileStore<K, V> implements NonBlockingStore<K, V> {
   private static final Log log = LogFactory.getLog(SingleFileStore.class);

   public static final byte[] MAGIC_BEFORE_11 = new byte[]{'F', 'C', 'S', '1'}; //<11
   public static final byte[] MAGIC_11_0 = new byte[]{'F', 'C', 'S', '2'};
   public static final byte[] MAGIC_12_0 = new byte[]{'F', 'C', 'S', '3'};
   public static final byte[] MAGIC_12_1 = new byte[]{'F', 'C', 'S', '4'};
   public static final byte[] MAGIC_LATEST = MAGIC_12_1;
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
   @GuardedBy("resizeLock")
   private Map<K, FileEntry>[] entries;
   private SortedSet<FileEntry> freeList;
   private long filePos;
   private File file;
   private float fragmentationFactor = .75f;
   // Prevent clear() from truncating the file after a write() allocated the entry but before it wrote the data
   private final StampedLock resizeLock = new StampedLock();
   private TimeService timeService;
   private MarshallableEntryFactory<K, V> entryFactory;
   private KeyPartitioner keyPartitioner;
   private BlockingManager blockingManager;
   private boolean segmented;
   private int actualNumSegments;
   private int maxEntries;

   public static File getStoreFile(String directoryPath, String cacheName) {
      return new File(new File(directoryPath), cacheName + ".dat");
   }

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
      this.timeService = ctx.getTimeService();
      this.entryFactory = ctx.getMarshallableEntryFactory();
      this.blockingManager = ctx.getBlockingManager();

      keyPartitioner = ctx.getKeyPartitioner();
      maxEntries = configuration.maxEntries();
      segmented = configuration.segmented();
      if (segmented) {
         actualNumSegments = ctx.getCache().getCacheConfiguration().clustering().hash().numSegments();
      } else {
         actualNumSegments = 1;
      }

      entries = new Map[actualNumSegments];
      freeList = Collections.synchronizedSortedSet(new TreeSet<>());

      // Not really blocking because no other thread can access the lock during start
      blockingAddSegments(IntSets.immutableRangeSet(actualNumSegments));
      return blockingManager.runBlocking(this::blockingStart, "sfs-start");
   }

   private void blockingStart() {
      boolean shouldClear = configuration.purgeOnStartup();
      boolean readOnly = configuration.ignoreModifications();
      assert !(shouldClear && readOnly) : "Store can't be configured with both purge and ignore modifications";

      try {
         Path resolvedPath = PersistenceUtil.getLocation(ctx.getGlobalConfiguration(), configuration.location());
         file = getStoreFile(resolvedPath.toString(), cacheName());
         boolean hasSingleFile = SecurityActions.fileExists(file);
         if (hasSingleFile) {
            channel = SecurityActions.openFileChannel(file);

            byte[] magicHeader = validateExistingFile(channel, file.getAbsolutePath());
            if (magicHeader != null) {
               migrateNonSegmented(magicHeader, shouldClear);
            } else if (!shouldClear) {
               rebuildIndex();
               processFreeEntries();
            } else {
               clear();
            }
         } else if (hasAnyComposedSegmentedFiles()) {
            if (!shouldClear) {
               migrateFromComposedSegmentedLoadWriteStore(shouldClear);
            }
         } else {
            // No existing files
            if (!readOnly) {
               File dir = file.getParentFile();
               if (!SecurityActions.createDirectoryIfNeeded(dir)) {
                  throw PERSISTENCE.directoryCannotBeCreated(dir.getAbsolutePath());
               }

               channel = createNewFile(file);
            }
         }

         // Initialize the fragmentation factor
         fragmentationFactor = configuration.fragmentationFactor();
      } catch (PersistenceException e) {
         throw e;
      } catch (Throwable t) {
         throw new PersistenceException(t);
      }
   }

   private boolean hasAnyComposedSegmentedFiles() {
      int numSegments = ctx.getCache().getCacheConfiguration().clustering().hash().numSegments();
      for (int segment = 0; segment < numSegments; segment++) {
         File segmentFile = getComposedSegmentFile(segment);
         if (SecurityActions.fileExists(segmentFile))
            return true;
      }
      return false;
   }

   /**
    * @return The magic bytes from the header or {@code null} if migration is not needed.
    */
   private byte[] validateExistingFile(FileChannel fileChannel, String filePath) throws Exception {
      // check file format and read persistent state if enabled for the cache
      byte[] headerMagic = new byte[MAGIC_LATEST.length];
      int headerBytes = fileChannel.read(ByteBuffer.wrap(headerMagic), 0);
      if (headerBytes == MAGIC_LATEST.length) {
         if (!Arrays.equals(MAGIC_LATEST, headerMagic))
            return headerMagic;
      } else {
         // Store file has less than MAGIC_LEN bytes
         throw PERSISTENCE.invalidSingleFileStoreData(filePath);
      }
      return null;
   }

   private void migrateNonSegmented(byte[] magicHeader, boolean removeOnly) throws Exception {
      PERSISTENCE.startMigratingPersistenceData(cacheName());
      File newFile = new File(file.getParentFile(), cacheName() + "_new.dat");
      try {
         if (SecurityActions.fileExists(newFile)) {
            if (log.isTraceEnabled()) log.tracef("Overwriting temporary migration file %s", newFile);
            // Delete file to overwrite permissions as well
            SecurityActions.deleteFile(newFile);
         }

         try (FileChannel newChannel = createNewFile(newFile)) {
            if (!removeOnly) {
               copyEntriesFromOldFile(magicHeader, newChannel, channel, file.toString());
            }
         }

         //close old file
         channel.close();
         //replace old file with the new file
         SecurityActions.moveFile(newFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
         //reopen the file
         channel = SecurityActions.openFileChannel(file);
         PERSISTENCE.persistedDataSuccessfulMigrated(cacheName());
      } catch (IOException e) {
         throw PERSISTENCE.persistedDataMigrationFailed(cacheName(), e);
      }
   }

   private void copyEntriesFromOldFile(byte[] magicHeader, FileChannel destChannel, FileChannel sourceChannel,
                                       String sourcePath) throws Exception {
      if (magicHeader == null) {
         // The segment file has the 12.1 magic header
         copyEntriesFromV12_0(destChannel, sourceChannel, sourcePath);
      } else if (Arrays.equals(MAGIC_12_0, magicHeader)) {
         if (ctx.getGlobalConfiguration().serialization().marshaller() == null) {
            // ISPN-13128 Upgrading to 12.0 with the default marshaller might have corrupted the file data
            copyCorruptDataV12_0(destChannel, sourceChannel, sourcePath);
         } else {
            // Data is not corrupt
            copyEntriesFromV12_0(destChannel, sourceChannel, sourcePath);
         }
      } else if (Arrays.equals(MAGIC_11_0, magicHeader)) {
         copyEntriesFromV11(destChannel, sourceChannel);
      } else if (Arrays.equals(MAGIC_BEFORE_11, magicHeader)) {
         throw PERSISTENCE.persistedDataMigrationUnsupportedVersion("< 11");
      } else {
         throw PERSISTENCE.invalidSingleFileStoreData(file.getAbsolutePath());
      }
   }

   private FileChannel createNewFile(File newFile) throws IOException {
      FileChannel newChannel = SecurityActions.openFileChannel(newFile);
      try {
         // Write Magic
         newChannel.truncate(0);
         newChannel.write(ByteBuffer.wrap(MAGIC_LATEST), 0);
         filePos = MAGIC_LATEST.length;
         return newChannel;
      } catch (Throwable t) {
         newChannel.close();
         throw t;
      }
   }

   @Override
   public CompletionStage<Void> stop() {
      return ctx.getBlockingManager().runBlocking(this::blockingStop, "sfs-stop");
   }

   private void blockingStop() {
      if (log.isTraceEnabled() && channel != null) {
         // Must compute the size before acquiring resizeLock
         Long size = CompletionStages.join(approximateSize(IntSets.immutableRangeSet(actualNumSegments)));
         log.tracef("Stopping store %s, size = %d, file size = %d", cacheName(), size, filePos);
      }
      long stamp = resizeLock.writeLock();
      try {
         if (channel != null) {
            // reset state
            channel.close();
            channel = null;
            entries = null;
            freeList = null;
         }
      } catch (Exception e) {
         throw new PersistenceException(e);
      } finally {
         resizeLock.unlockWrite(stamp);
      }
   }

   @Override
   public Set<Characteristic> characteristics() {
      return EnumSet.of(Characteristic.BULK_READ, Characteristic.EXPIRATION, Characteristic.SEGMENTABLE);
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      return CompletableFutures.booleanStage(SecurityActions.fileExists(file));
   }

   /**
    * Rebuilds the in-memory index from file.
    */
   private void rebuildIndex() throws Exception {
      filePos = MAGIC_LATEST.length;
      ByteBuffer buf = ByteBuffer.allocate(KEY_POS_LATEST);
      for (; ; ) {
         // read FileEntry fields from file (size, keyLen etc.)
         buf = readChannel(buf, filePos, KEY_POS_LATEST, channel);
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
            buf = readChannel(buf, fe.offset + KEY_POS_LATEST, fe.keyLen, channel);

            // deserialize key and add to entries map
            // Marshaller should allow for provided type return for safety
            K key = (K) ctx.getPersistenceMarshaller().objectFromByteBuffer(buf.array(), 0, fe.keyLen);
            // We start by owning all the segments
            Map<K, FileEntry> segmentEntries = getSegmentEntries(getSegment(key));
            segmentEntries.put(key, fe);
         } else {
            // add to free list
            freeList.add(fe);
         }
      }
   }

   private int getSegment(Object key) {
      return segmented ? keyPartitioner.getSegment(key) : 0;
   }

   // Initialise missing internal metadata state for corrupt data
   private PrivateMetadata generateMissingInternalMetadata() {
      // Optimistic Transactions
      AdvancedCache<?, ?> cache = ctx.getCache().getAdvancedCache();
      Configuration config = cache.getCacheConfiguration();
      TransactionConfiguration txConfig = config.transaction();
      PrivateMetadata.Builder builder = new PrivateMetadata.Builder();
      if (txConfig.transactionMode() == TransactionMode.TRANSACTIONAL && txConfig.lockingMode() == LockingMode.OPTIMISTIC) {
         builder.entryVersion(new SimpleClusteredVersion(1, 1));
      }

      // Async XSite
      if (config.sites().hasAsyncEnabledBackups()) {
         String siteName = cache.getCacheManager().getTransport().localSiteName();
         IracEntryVersion version = new IracEntryVersion(Collections.singletonMap(siteName, TopologyIracVersion.newVersion(1)));
         builder.iracMetadata(new IracMetadata(siteName, version));
      }
      return builder.build();
   }

   private void migrateFromComposedSegmentedLoadWriteStore(boolean removeOnly) throws IOException {
      PERSISTENCE.startMigratingPersistenceData(cacheName());
      File newFile = new File(file.getParentFile(), cacheName() + "_new.dat");
      try {
         if (SecurityActions.fileExists(newFile)) {
            if (log.isTraceEnabled()) log.tracef("Overwriting temporary migration file %s", newFile);
            // Delete file to overwrite permissions as well
            SecurityActions.deleteFile(newFile);
         }

         try (FileChannel newChannel = createNewFile(newFile)) {
            if (!removeOnly) {
               copyEntriesFromOldSegmentFiles(newChannel);
            }
         }

         // Move the new file to the final name
         SecurityActions.moveFile(newFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);

         // Reopen the file
         channel = SecurityActions.openFileChannel(file);

         // Remove the composed segment files
         removeComposedSegmentedLoadWriteStoreFiles();

         PERSISTENCE.persistedDataSuccessfulMigrated(cacheName());
      } catch (PersistenceException e) {
         throw e;
      } catch (Exception e) {
         throw PERSISTENCE.persistedDataMigrationFailed(cacheName(), e);
      }
   }

   private void copyEntriesFromOldSegmentFiles(FileChannel newChannel) throws Exception {
      int numSegments = ctx.getCache().getCacheConfiguration().clustering().hash().numSegments();
      for (int segment = 0; segment < numSegments; segment++) {
         File segmentFile = getComposedSegmentFile(segment);
         if (SecurityActions.fileExists(segmentFile)) {
            try (FileChannel segmentChannel = SecurityActions.openFileChannel(segmentFile)) {
               byte[] magic = validateExistingFile(segmentChannel, segmentFile.toString());
               copyEntriesFromOldFile(magic, newChannel, segmentChannel, segmentFile.toString());
            }
         }
      }
   }

   private void removeComposedSegmentedLoadWriteStoreFiles() {
      Path rootLocation = PersistenceUtil.getLocation(ctx.getGlobalConfiguration(), configuration.location());
      if (log.isTraceEnabled()) log.tracef("Removing old ComposedSegmentedLoadWriteStore files from %s", rootLocation);
      int numSegments = ctx.getCache().getCacheConfiguration().clustering().hash().numSegments();
      for (int segment = 0; segment < numSegments; segment++) {
         File segmentFile = getComposedSegmentFile(segment);
         if (SecurityActions.fileExists(segmentFile)) {
            SecurityActions.deleteFile(segmentFile);
            if (segmentFile.getParentFile() != null && segmentFile.isDirectory() &&
                segmentFile.getParentFile().list().length == 0) {
               // The segment directory is empty, remove it
               SecurityActions.deleteFile(segmentFile.getParentFile());
            }
         }
      }
   }

   private File getComposedSegmentFile(int segment) {
      return segmentFileLocation(ctx.getGlobalConfiguration(), configuration.location(), cacheName(), segment);
   }

   private static File segmentFileLocation(GlobalConfiguration globalConfiguration, String location,
                                           String cacheName, int segment) {
      Path rootPath = PersistenceUtil.getLocation(globalConfiguration, location);
      String segmentPath = AbstractSegmentedStoreConfiguration.fileLocationTransform(rootPath.toString(), segment);
      return getStoreFile(segmentPath, cacheName);
   }

   private void copyEntriesFromV12_0(FileChannel destChannel, FileChannel sourceChannel, String sourcePath) throws Exception {
      try {
         long currentTs = timeService.wallClockTime();
         ByteBuffer buf = ByteBuffer.allocate(KEY_POS_LATEST);
         ByteBuffer bodyBuf = ByteBuffer.allocate(KEY_POS_LATEST);

         long oldFilePos = MAGIC_12_0.length;
         while (true) {
            // read FileEntry fields from file (size, keyLen etc.)
            buf = readChannel(buf, oldFilePos, KEY_POS_11_0, sourceChannel);
            if (buf.remaining() > 0)
               break;

            buf.flip();

            // initialize FileEntry from buffer
            FileEntry oldFe = new FileEntry(oldFilePos, buf);

            // sanity check
            if (oldFe.size < KEY_POS_11_0 + oldFe.keyLen + oldFe.dataLen + oldFe.metadataLen + oldFe.internalMetadataLen) {
               throw PERSISTENCE.errorReadingFileStore(file.getPath(), oldFilePos);
            }

            //update old file pos to the next entry
            oldFilePos += oldFe.size;

            // check if the entry is used or free
            // if it is free, it is ignored.
            if (oldFe.keyLen < 1)
               continue;

            // The entry has already expired, so avoid writing to the new file
            if (oldFe.expiryTime > 0 && oldFe.expiryTime < currentTs)
               continue;

            // Read the body of the entry, skipping the fixed header
            bodyBuf = allocate(bodyBuf, oldFe.size - KEY_POS_LATEST);
            readChannel(bodyBuf, oldFe.offset + KEY_POS_LATEST, oldFe.size - KEY_POS_LATEST, sourceChannel);

            K key = (K) ctx.getPersistenceMarshaller().objectFromByteBuffer(bodyBuf.array(), 0, oldFe.keyLen);

            // Update the entry with the destination filePos
            FileEntry newFe = new FileEntry(this.filePos, oldFe.size, oldFe.keyLen, oldFe.dataLen, oldFe.metadataLen, oldFe.internalMetadataLen, oldFe.expiryTime);
            // Put the updated entry in the entries map so we don't need to rebuild the index later
            Map<K, FileEntry> segmentEntries = getSegmentEntries(getSegment(key));
            segmentEntries.put(key, newFe);

            buf.flip();
            destChannel.write(buf, this.filePos);
            bodyBuf.flip();
            destChannel.write(bodyBuf, this.filePos + KEY_POS_LATEST);
            this.filePos += newFe.size;
            if (log.isTraceEnabled())
               log.tracef("Recovered entry %s at %d:%d", key, newFe.size, newFe.offset, newFe.size);
         }
      } catch (IOException e) {
         throw PERSISTENCE.persistedDataMigrationFailed(cacheName(), e);
      }
   }

   private String cacheName() {
      return ctx.getCache().getName();
   }

   private void copyCorruptDataV12_0(FileChannel destChannel, FileChannel sourceChannel, String sourcePath) {
      PERSISTENCE.startRecoveringCorruptPersistenceData(sourcePath);

      // Day before the release of Infinispan 10.0.0.Final
      // This was the first release that SFS migrations on startup could be migrated from via ISPN 10 -> 11 -> 12
      long sanityEpoch = LocalDate.of(2019, 10, 26)
                                  .atStartOfDay()
                                  .atZone(ZoneId.systemDefault())
                                  .toInstant()
                                  .toEpochMilli();
      long currentTs = timeService.wallClockTime();

      int entriesRecovered = 0;
      ByteBuffer buf = ByteBuffer.allocate(KEY_POS_LATEST);
      ByRef<ByteBuffer> bufRef = ByRef.create(buf);
      try {
         long fileSize = sourceChannel.size();
         long oldFilePos = MAGIC_12_0.length;
         while (true) {
            buf = readChannel(buf, oldFilePos, KEY_POS_LATEST, sourceChannel);
            // EOF reached
            if (buf.remaining() > 0)
               break;

            buf.flip();
            FileEntry fe = new FileEntry(oldFilePos, buf);
            // Semantic check to find valid FileEntry
            // fe.keyLen = 0 is valid, however it means we should skip the entry
            if (fe.size <= 0 || fe.expiryTime < -1 ||
                  fe.keyLen <= 0 || fe.keyLen > fe.size ||
                  fe.dataLen <= 0 || fe.dataLen > fe.size ||
                  fe.metadataLen < 0 || fe.metadataLen > fe.size ||
                  fe.internalMetadataLen < 0 || fe.internalMetadataLen > fe.size) {
               // Check failed, try to read FileEntry from the next byte
               oldFilePos++;
               continue;
            }

            // Extra check to prevent buffers being created that exceed the remaining number of bytes in the file
            long estimateSizeExcludingInternal = fe.keyLen;
            estimateSizeExcludingInternal += fe.dataLen;
            estimateSizeExcludingInternal += fe.metadataLen;
            if (estimateSizeExcludingInternal > fileSize - oldFilePos) {
               oldFilePos++;
               continue;
            }

            K key;
            V value;
            Metadata metadata = null;
            ByRef.Long offset = new ByRef.Long(oldFilePos + KEY_POS_LATEST);
            bufRef.set(buf);
            try {
               // Read old entry content and then write
               key = unmarshallObject(bufRef, offset, fe.keyLen, sourceChannel);
               value = unmarshallObject(bufRef, offset, fe.dataLen, sourceChannel);

               int metaLen = fe.metadataLen > 0 ? fe.metadataLen - TIMESTAMP_BYTES : 0;
               if (metaLen > 0)
                  metadata = unmarshallObject(bufRef, offset, metaLen, sourceChannel);

               // Entries successfully unmarshalled so it's safe to increment oldFilePos to FileEntry+offset so brute-force can resume on next iteration
               oldFilePos = offset.get();
            } catch(Throwable t) {
               // Must have been a false positive FileEntry. Increment oldFilePos by 1 bytes and retry
               oldFilePos++;
               continue;
            } finally {
               buf = bufRef.get();
            }


            long created = -1;
            long lastUsed = -1;
            if (fe.metadataLen > 0 && fe.expiryTime > 0) {
               buf = readChannelUpdateOffset(buf, offset, TIMESTAMP_BYTES, sourceChannel);
               buf.flip();
               // Try to read timestamps. If corrupt data then this could be nonsense
               created = buf.getLong();
               lastUsed = buf.getLong();

               // If the Timestamps are in the future or < sanityEpoch, then we're migrating corrupt data so set the value to current wallClockTime
               if (created != -1 && (created > currentTs || created < sanityEpoch)) {
                  long lifespan = metadata.lifespan();
                  created = lifespan > 0 ? fe.expiryTime - lifespan : currentTs;
               }

               if (lastUsed != -1 && (lastUsed > currentTs || lastUsed < sanityEpoch)) {
                  long maxIdle = metadata.maxIdle();
                  lastUsed = maxIdle > 0 ? fe.expiryTime - maxIdle : currentTs;
               }

               oldFilePos = offset.get();
            }

            PrivateMetadata internalMeta = null;
            if (fe.internalMetadataLen > 0) {
               try {
                  bufRef.set(buf);
                  internalMeta = unmarshallObject(bufRef, offset, fe.internalMetadataLen, sourceChannel);
                  oldFilePos = offset.get();
               } catch (Throwable t) {
                  // Will fail if data is corrupt as PrivateMetadata doesn't exist
                  internalMeta = generateMissingInternalMetadata();
               } finally {
                  buf = bufRef.get();
               }
            }

            // Last expiration check before writing as expiryTime is considered good now
            // This check is required as write below doesn't verify expiration or not and
            // just creates a new expiryTime.
            if (fe.expiryTime > 0 && fe.expiryTime < currentTs) {
               continue;
            }

            MarshallableEntry<? extends K, ? extends V> me = (MarshallableEntry<? extends K, ? extends V>) ctx.getMarshallableEntryFactory().create(key, value, metadata, internalMeta, created, lastUsed);
            write(getSegment(key), me, destChannel);
            entriesRecovered++;
         }
         if (log.isTraceEnabled()) log.tracef("Recovered %d entries", entriesRecovered);
      } catch (IOException e) {
         throw PERSISTENCE.corruptDataMigrationFailed(cacheName(), e);
      }
   }

   private void copyEntriesFromV11(FileChannel destChannel, FileChannel sourceChannel) {
      long oldFilePos = MAGIC_11_0.length;
      // Only update the key/value/meta bytes if the default marshaller is configured
      boolean wrapperMissing = ctx.getGlobalConfiguration().serialization().marshaller() == null;

      try {
         long currentTs = timeService.wallClockTime();
         ByteBuffer buf = ByteBuffer.allocate(KEY_POS_LATEST);
         ByRef<ByteBuffer> bufRef = ByRef.create(buf);
         for (; ; ) {
            // read FileEntry fields from file (size, keyLen etc.)
            buf = readChannel(buf, oldFilePos, KEY_POS_11_0, sourceChannel);
            if (buf.remaining() > 0)
               break;

            buf.flip();

            // initialize FileEntry from buffer
            FileEntry oldFe = new FileEntry(oldFilePos, buf);

            // sanity check
            if (oldFe.size < KEY_POS_11_0 + oldFe.keyLen + oldFe.dataLen + oldFe.metadataLen + oldFe.internalMetadataLen) {
               throw PERSISTENCE.errorReadingFileStore(file.getPath(), oldFilePos);
            }

            //update old file pos to the next entry
            oldFilePos += oldFe.size;

            // check if the entry is used or free
            // if it is free, it is ignored.
            if (oldFe.keyLen < 1)
               continue;

            // The entry has already expired, so avoid writing to the new file
            if (oldFe.expiryTime > 0 && oldFe.expiryTime < currentTs)
               continue;

            ByRef.Long offset = new ByRef.Long(oldFe.offset + KEY_POS_11_0);

            long created = -1;
            long lastUsed = -1;
            bufRef.set(buf);
            K key = unmarshallObject(bufRef, offset, oldFe.keyLen, wrapperMissing, sourceChannel);
            V value = unmarshallObject(bufRef, offset, oldFe.dataLen, wrapperMissing, sourceChannel);
            Metadata metadata = null;
            if (oldFe.metadataLen > 0) {
               metadata = unmarshallObject(bufRef, offset, oldFe.metadataLen - TIMESTAMP_BYTES, wrapperMissing, sourceChannel);

               if (oldFe.expiryTime > 0) {
                  buf = bufRef.get();
                  buf = readChannelUpdateOffset(buf, offset, TIMESTAMP_BYTES, sourceChannel);
                  buf.flip();
                  created = buf.getLong();
                  lastUsed = buf.getLong();
                  bufRef.set(buf);
               }
            }

            PrivateMetadata internalMeta = null;
            if (oldFe.internalMetadataLen > 0) {
               internalMeta = unmarshallObject(bufRef, offset, oldFe.internalMetadataLen, wrapperMissing, sourceChannel);
               buf = bufRef.get();
            }
            MarshallableEntry<? extends K, ? extends V> me = (MarshallableEntry<? extends K, ? extends V>) ctx.getMarshallableEntryFactory()
                  .create(key, value, metadata, internalMeta, created, lastUsed);
            write(getSegment(key), me, destChannel);
         }
      } catch (IOException | ClassNotFoundException e) {
         throw PERSISTENCE.persistedDataMigrationFailed(cacheName(), e);
      }
   }

   private <T> T unmarshallObject(ByRef<ByteBuffer> buf, ByRef.Long offset, int length, FileChannel sourceChannel) throws ClassNotFoundException, IOException {
      return unmarshallObject(buf, offset, length, false, sourceChannel);
   }

   @SuppressWarnings("unchecked")
   private <T> T unmarshallObject(ByRef<ByteBuffer> bufRef, ByRef.Long offset, int length, boolean legacyWrapperMissing,
                                  FileChannel sourceChannel) throws ClassNotFoundException, IOException {
      ByteBuffer buf = bufRef.get();
      buf = readChannelUpdateOffset(buf, offset, length, sourceChannel);
      byte[] bytes = buf.array();
      bufRef.set(buf);

      PersistenceMarshaller persistenceMarshaller = ctx.getPersistenceMarshaller();
      if (legacyWrapperMissing) {
         // Read using raw user marshaller without MarshallUserObject wrapping
         Marshaller marshaller = persistenceMarshaller.getUserMarshaller();
         try {
            return (T) marshaller.objectFromByteBuffer(bytes, 0, length);
         } catch (IllegalArgumentException e) {
            // For internal cache key/values and custom metadata we need to use the persistence marshaller
            return (T) persistenceMarshaller.objectFromByteBuffer(bytes, 0, length);
         }
      }
      return(T) persistenceMarshaller.objectFromByteBuffer(bytes, 0, length);
   }

   private ByteBuffer readChannelUpdateOffset(ByteBuffer buf, ByRef.Long offset, int length, FileChannel sourceChannel) throws IOException {
      return readChannel(buf, offset.getAndAdd(length), length, sourceChannel);
   }

   private ByteBuffer readChannel(ByteBuffer buf, long offset, int length, FileChannel channel) throws IOException {
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
    * The base class implementation calls {@link #load(int, Object)} for this, we can do better because
    * we keep all keys in memory.
    */
   @Override
   public CompletionStage<Boolean> containsKey(int segment, Object key) {
      // Avoid switching threads if there is nothing to load
      long stamp = resizeLock.tryReadLock();
      if (stamp != 0) {
         // Acquires the FileEntry lock and releases the read lock
         FileEntry fe = getFileEntryWithReadLock(segment, key, stamp, false);
         return CompletableFutures.booleanStage(fe != null);
      }
      // Someone is holding the write lock
      return blockingManager.supplyBlocking(() -> blockingContainsKey(segment, key), "sfs-containsKey");
   }

   private boolean blockingContainsKey(int segment, Object key) {
      long stamp = resizeLock.readLock();
      FileEntry fe = getFileEntryWithReadLock(segment, key, stamp, false);
      return fe != null;
   }

   /**
    * Allocates the requested space in the file.
    *
    * @param len requested space
    * @return allocated file position and length as FileEntry object
    */
   @GuardedBy("resizeLock.readLock()")
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
         // Wait for any reader to finish
         fe.waitUnlocked();

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
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> marshalledEntry) {
      return blockingManager.runBlocking(() -> blockingWrite(segment, marshalledEntry), "sfs-write");
   }

   private void blockingWrite(int segment, MarshallableEntry<? extends K, ? extends V> marshalledEntry) {
      write(segment, marshalledEntry, channel);
   }

   private void write(int segment, MarshallableEntry<? extends K, ? extends V> marshalledEntry, FileChannel channel) {
      // serialize cache value
      org.infinispan.commons.io.ByteBuffer key = marshalledEntry.getKeyBytes();
      org.infinispan.commons.io.ByteBuffer data = marshalledEntry.getValueBytes();
      org.infinispan.commons.io.ByteBuffer metadata = marshalledEntry.getMetadataBytes();
      org.infinispan.commons.io.ByteBuffer internalMetadata = marshalledEntry.getInternalMetadataBytes();

      // allocate file entry and store in cache file
      int metadataLength = metadata == null ? 0 : metadata.getLength() + TIMESTAMP_BYTES;
      int internalMetadataLength = internalMetadata == null ? 0 : internalMetadata.getLength();
      int len = KEY_POS_LATEST + key.getLength() + data.getLength() + metadataLength + internalMetadataLength;

      long stamp = resizeLock.readLock();
      try {
         Map<K, FileEntry> segmentEntries = getSegmentEntries(segment);
         if (segmentEntries == null) {
            // We don't own the segment
            return;
         }

         FileEntry newEntry = allocate(len);
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
         if (log.isTraceEnabled())
            log.tracef("Wrote entry %s:%d at %d:%d", marshalledEntry.getKey(), len, newEntry.offset, newEntry.size);

         // add the new entry to in-memory index
         FileEntry oldEntry = segmentEntries.put(marshalledEntry.getKey(), newEntry);

         // if we added an entry, check if we need to evict something
         if (oldEntry == null)
            oldEntry = evict();

         // in case we replaced or evicted an entry, add to freeList
         free(oldEntry);
      } catch (Exception e) {
         throw new PersistenceException(e);
      } finally {
         resizeLock.unlockRead(stamp);
      }
   }

   /**
    * Try to evict an entry if the capacity of the cache store is reached.
    *
    * @return FileEntry to evict, or null (if unbounded or capacity is not yet reached)
    */
   @GuardedBy("resizeLock#readLock")
   private FileEntry evict() {
      if (maxEntries > 0) {
         // When eviction is enabled, segmentation is disabled
         Map<K, FileEntry> segment0Entries = getSegmentEntries(0);
         synchronized (segment0Entries) {
            if (segment0Entries.size() > maxEntries) {
               Iterator<FileEntry> it = segment0Entries.values().iterator();
               FileEntry fe = it.next();
               it.remove();
               return fe;
            }
         }
      }
      return null;
   }

   @Override
   public CompletionStage<Void> clear() {
      return blockingManager.runBlocking(this::blockingClear, "sfs-clear");
   }

   private void blockingClear() {
      long stamp = resizeLock.writeLock();
      try {
         // Wait until all readers are done reading all file entries
         // First, used entries
         for (Map<K, FileEntry> segmentEntries : entries) {
            if (segmentEntries == null)
               continue;

            synchronized (segmentEntries) {
               for (FileEntry fe : segmentEntries.values())
                  fe.waitUnlocked();

               segmentEntries.clear();
            }
         }

         // Then free entries that others might still be reading
         synchronized (freeList) {
            for (FileEntry fe : freeList)
               fe.waitUnlocked();

            // clear in-memory state
            freeList.clear();
         }

         // All readers are done, reset file
         if (log.isTraceEnabled()) log.tracef("Truncating file, current size is %d", filePos);
         channel.truncate(4);
         channel.write(ByteBuffer.wrap(MAGIC_LATEST), 0);
         filePos = MAGIC_LATEST.length;
      } catch (Exception e) {
         throw new PersistenceException(e);
      } finally {
         resizeLock.unlockWrite(stamp);
      }
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      long stamp = resizeLock.tryReadLock();
      if (stamp != 0) {
         FileEntry fe = deleteWithReadLock(segment, key);
         if (fe == null) {
            resizeLock.unlockRead(stamp);
            return CompletableFutures.completedFalse();
         }

         return blockingManager.supplyBlocking(() -> deleteInFile(stamp, fe), "sfs-delete");
      }

      return blockingManager.supplyBlocking(() -> blockingDelete(segment, key), "sfs-delete");
   }

   private boolean blockingDelete(int segment, Object key) {
      long stamp = resizeLock.readLock();
      FileEntry fe = deleteWithReadLock(segment, key);
      return deleteInFile(stamp, fe);
   }

   /**
    * Mark the entry as deleted on disk and release the read lock.
    */
   private boolean deleteInFile(long stamp, FileEntry fe) {
      try {
         free(fe);
         return fe != null;
      } catch (Exception e) {
         throw new PersistenceException(e);
      } finally {
         resizeLock.unlockRead(stamp);
      }
   }

   private FileEntry deleteWithReadLock(int segment, Object key) {
      Map<K, FileEntry> segmentEntries = getSegmentEntries(segment);
      if (segmentEntries == null) {
         // We don't own the segment
         return null;
      }

      return segmentEntries.remove(key);
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      // Avoid switching threads if there is nothing to load
      long stamp = resizeLock.tryReadLock();
      if (stamp != 0) {
         // Acquires the FileEntry lock and releases the read lock
         FileEntry fe = getFileEntryWithReadLock(segment, key, stamp, true);
         if (fe == null) {
            return CompletableFutures.completedNull();
         }

         // Perform the actual read holding only the FileEntry lock
         return blockingManager.supplyBlocking(() -> readFromDisk(fe, key, true, true), "sfs-load");
      }
      // Someone is holding the write lock
      return blockingManager.supplyBlocking(() -> blockingLoad(segment, key, true, true), "sfs-load");
   }

   private MarshallableEntry<K, V> blockingLoad(int segment, Object key, boolean loadValue, boolean loadMetadata) {
      long stamp = resizeLock.readLock();
      FileEntry fe = getFileEntryWithReadLock(segment, key, stamp, true);
      if (fe == null)
         return null;

      // Perform the actual read holding only the FileEntry lock
      return readFromDisk(fe, key, loadValue, loadMetadata);
   }

   /**
    * Get the file entry from the segment map and release the read lock
    */
   private FileEntry getFileEntryWithReadLock(int segment, Object key, long stamp, boolean lockFileEntry) {
      final FileEntry fe;
      try {
         Map<K, FileEntry> segmentEntries = getSegmentEntries(segment);
         if (segmentEntries == null)
            return null;

         synchronized (segmentEntries) {
            // lookup FileEntry of the key
            fe = segmentEntries.get(key);
            if (fe == null)
               return null;

            // Entries are removed due to expiration from {@link SingleFileStore#purge}
            if (fe.isExpired(timeService.wallClockTime())) {
               return null;
            } else if (lockFileEntry) {
               // lock entry for reading before releasing entries monitor
               fe.lock();
            }
         }
      } finally {
         resizeLock.unlockRead(stamp);
      }
      return fe;
   }

   private MarshallableEntry<K, V> readFromDisk(FileEntry fe, Object key, boolean loadValue, boolean loadMetadata) {
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

   /**
    * @return The entries of a segment, or {@code null} if the segment is not owned
    */
   @GuardedBy("resizeLock")
   private Map<K, FileEntry> getSegmentEntries(int segment) {
      if (!segmented) {
         return entries[0];
      }

      // Segmented
      if (actualNumSegments <= segment) {
         throw new IndexOutOfBoundsException();
      }
      return entries[segment];
   }

   @Override
   public Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      if (!segmented) {
         return publishSegmentKeys(k -> keyMatches(segments, filter, k), 0);
      }
      return Flowable.fromIterable(segments)
                     .concatMap(segment -> publishSegmentKeys(filter, segment));
   }

   private Publisher<K> publishSegmentKeys(Predicate<? super K> filter, int segment) {
      long stamp = resizeLock.tryReadLock();
      if (stamp != 0) {
         return publishSegmentKeysWithReadLock(filter, segment, stamp);
      } else {
         return blockingManager.blockingPublisher(defer(() -> {
            long stamp1 = resizeLock.readLock();
            return publishSegmentKeysWithReadLock(filter, segment, stamp1);
         }));
      }
   }

   private boolean keyMatches(IntSet segments, Predicate<? super K> filter, K k) {
      return segments.contains(keyPartitioner.getSegment(k)) && (filter == null || filter.test(k));
   }

   private Flowable<K> publishSegmentKeysWithReadLock(Predicate<? super K> filter, int segment, long stamp) {
      try {
         Map<K, FileEntry> segmentEntries = getSegmentEntries(segment);
         if (segmentEntries == null) {
            return Flowable.empty();
         }

         List<K> keys;
         long now = ctx.getTimeService().wallClockTime();
         synchronized (segmentEntries) {
            keys = new ArrayList<>(segmentEntries.size());
            for (Map.Entry<K, FileEntry> e : segmentEntries.entrySet()) {
               K key = e.getKey();
               if (e.getValue().isExpired(now))
                  continue;
               if (filter != null && !filter.test(key))
                  continue;
               keys.add(key);
            }
         }
         return Flowable.fromIterable(keys);
      } finally {
         resizeLock.unlockRead(stamp);
      }
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter,
                                                            boolean includeValues) {
      if (!segmented) {
         return publishSegmentEntries(0, k -> keyMatches(segments, filter, k), includeValues);
      }

      return Flowable.fromIterable(segments)
                     .concatMap(segment -> publishSegmentEntries(segment, filter, includeValues));
   }

   private Publisher<MarshallableEntry<K, V>> publishSegmentEntries(int segment, Predicate<? super K> filter,
                                                                    boolean includeValues) {
      long stamp = resizeLock.tryReadLock();
      if (stamp != 0 && getSegmentEntries(segment) == null) {
         resizeLock.unlockRead(stamp);
         return Flowable.empty();
      }
      return blockingManager.blockingPublisher(defer(() -> {
         return blockingPublishSegmentEntries(segment, filter, includeValues, stamp);
      }));
   }

   private Flowable<MarshallableEntry<K, V>> blockingPublishSegmentEntries(int segment, Predicate<? super K> filter,
                                                                          boolean includeValues, long stamp) {
      List<KeyValuePair<K, FileEntry>> keysToLoad;
      long now = ctx.getTimeService().wallClockTime();
      if (stamp == 0) {
         // tryReadLock() did not acquire the lock
         stamp = resizeLock.readLock();
      }
      try {
         Map<K, FileEntry> segmentEntries = getSegmentEntries(segment);
         if (segmentEntries == null) {
            return Flowable.empty();
         }

         synchronized (segmentEntries) {
            // This way the sorting of entries is lazily done on each invocation of the publisher
            keysToLoad = new ArrayList<>(segmentEntries.size());
            for (Map.Entry<K, FileEntry> e : segmentEntries.entrySet()) {
               if (e.getValue().isExpired(now))
                  continue;
               if (filter != null && !filter.test(e.getKey()))
                  continue;
               keysToLoad.add(new KeyValuePair<>(e.getKey(), e.getValue()));
            }
         }
      } finally {
         resizeLock.unlockRead(stamp);
      }

      keysToLoad.sort(Comparator.comparingLong(o -> o.getValue().offset));
      return Flowable.fromIterable(keysToLoad).map(kvp -> {
         MarshallableEntry<K, V> entry = blockingLoad(segment, kvp.getKey(), includeValues, true);
         if (entry == null) {
            // Rxjava2 doesn't allow nulls
            entry = entryFactory.getEmpty();
         }
         return entry;
      }).filter(me -> me != entryFactory.getEmpty());
   }

   /**
    * Manipulates the free entries for optimizing disk space.
    */
   private void processFreeEntries() {
      long stamp = resizeLock.readLock();
      try {
         synchronized (freeList) {
            // Get a reverse sorted list of free entries based on file offset (bigger entries will be ahead of smaller entries)
            // This helps to work backwards with free entries at end of the file
            List<FileEntry> l = new ArrayList<>(freeList);
            l.sort(Comparator.comparingLong(fe -> -fe.offset));

            truncateFile(l);
            mergeFreeEntries(l);
         }
      } finally {
         resizeLock.unlockRead(stamp);
      }
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
      for (ListIterator<FileEntry> it = entries.listIterator(); it.hasNext(); ) {
         FileEntry fe = it.next();
         // Till we have free entries at the end of the file,
         // we can remove them and contract the file to release disk
         // space.
         if (!fe.isLocked() && ((fe.offset + fe.size) == filePos)) {
            truncateOffset = fe.offset;
            filePos = fe.offset;
            freeList.remove(fe);
            // Removing the entry would require moving all the elements, which is expensive
            it.set(null);
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
         // truncateFile sets entries to null instead of removing them
         if (fe == null || fe.isLocked())
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
   public Publisher<MarshallableEntry<K, V>> purgeExpired() {
      UnicastProcessor<MarshallableEntry<K, V>> processor = UnicastProcessor.create();
      blockingManager.runBlocking(() -> blockingPurgeExpired(processor), "sfs-purgeExpired");
      return processor;
   }

   private void blockingPurgeExpired(UnicastProcessor<MarshallableEntry<K, V>> processor) {
      try {
         long now = timeService.wallClockTime();
         for (int segment = 0; segment < actualNumSegments; segment++) {
            List<KeyValuePair<Object, FileEntry>> entriesToPurge;
            long stamp = resizeLock.readLock();
            try {
               Map<K, FileEntry> segmentEntries = getSegmentEntries(segment);
               if (segmentEntries == null)
                  continue;

               entriesToPurge = collectExpiredEntries(now, segmentEntries);
            } finally {
               resizeLock.unlockRead(stamp);
            }

            purgeExpiredEntries(now, processor, entriesToPurge);
         }

         // Disk space optimizations
         processFreeEntries();
      } catch (Throwable t) {
         processor.onError(t);
      } finally {
         processor.onComplete();
      }
   }

   private void purgeExpiredEntries(long now, UnicastProcessor<MarshallableEntry<K, V>> processor,
                                    List<KeyValuePair<Object, FileEntry>> entriesToPurge) {
      entriesToPurge.sort(Comparator.comparingLong(kvp -> kvp.getValue().offset));
      for (ListIterator<KeyValuePair<Object, FileEntry>> it = entriesToPurge.listIterator(); it.hasNext(); ) {
         KeyValuePair<Object, FileEntry> next = it.next();
         FileEntry fe = next.getValue();
         if (fe.isExpired(now)) {
            it.set(null);

            // Safe to unlock because the entry was locked collectExpiredEntries
            MarshallableEntry<K, V> entry = readFromDisk(fe, next.getKey(), true, true);
            processor.onNext(entry);

            try {
               free(fe);
            } catch (Exception e) {
               throw new PersistenceException(e);
            }
         }
      }
   }

   @GuardedBy("resizeLock")
   private List<KeyValuePair<Object, FileEntry>> collectExpiredEntries(long now, Map<K, FileEntry> segmentEntries) {
      List<KeyValuePair<Object, FileEntry>> entriesToPurge = new ArrayList<>();
      synchronized (segmentEntries) {
         for (Iterator<Map.Entry<K, FileEntry>> it = segmentEntries.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<K, FileEntry> next = it.next();
            FileEntry fe = next.getValue();
            if (fe.isExpired(now)) {
               it.remove();
               // We don't have to worry about other operations freeing the entry while we are reading it,
               // but we have to lock because readFromDisk() unlocks
               fe.lock();
               entriesToPurge.add(new KeyValuePair<>(next.getKey(), fe));
            }
         }
      }
      return entriesToPurge;
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      return Flowable.fromPublisher(publishKeys(segments, null)).count().toCompletionStage();
   }

   @Override
   public CompletionStage<Long> approximateSize(IntSet segments) {
      if (!segmented && segments.size() < ctx.getCache().getCacheConfiguration().clustering().hash().numSegments()) {
         return size(segments);
      }
      return blockingManager.supplyBlocking(() -> blockingApproximateSize(segments), "sfs-approximateSize");
   }

   private long blockingApproximateSize(IntSet segments) {
      long size = 0;
      long stamp = resizeLock.readLock();
      try {
         if (!segmented) {
            return getSegmentEntries(0).size();
         }

         for (PrimitiveIterator.OfInt iterator = segments.iterator(); iterator.hasNext(); ) {
            int segment = iterator.next();
            Map<K, FileEntry> segmentEntries = getSegmentEntries(segment);
            if (segmentEntries != null) {
               size += segmentEntries.size();
            }
         }
      } finally {
         resizeLock.unlockRead(stamp);
      }
      return size;
   }

   Map<K, FileEntry> getEntries() {
      long stamp = resizeLock.readLock();
      try {
         return Arrays.stream(entries)
                      .flatMap(segmentEntries -> segmentEntries != null ?
                                               segmentEntries.entrySet().stream() :
                                               Stream.empty())
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      } finally {
         resizeLock.unlockRead(stamp);
      }
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

   @Override
   public CompletionStage<Void> addSegments(IntSet segments) {
      if (!segmented) {
         throw new UnsupportedOperationException();
      }
      return blockingManager.runBlocking(() -> blockingAddSegments(segments), "sfs-addSegments");
   }

   private void blockingAddSegments(IntSet segments) {
      long stamp = resizeLock.writeLock();
      try {
         for (int segment : segments) {
            if (entries[segment] != null)
               continue;

            // Only use LinkedHashMap (LRU) for entries when cache store is bounded
            Map<K, FileEntry> entryMap = configuration.maxEntries() > 0 ?
                                         new LinkedHashMap<>(16, 0.75f, true) :
                                         new HashMap<>();
            entries[segment] = Collections.synchronizedMap(entryMap);
         }
      } finally {
         resizeLock.unlockWrite(stamp);
      }
   }

   @Override
   public CompletionStage<Void> removeSegments(IntSet segments) {
      if (!segmented) {
         throw new UnsupportedOperationException();
      }
      return blockingManager.runBlocking(() -> blockingRemoveSegments(segments), "sfs-removeSegments");
   }

   private void blockingRemoveSegments(IntSet segments) {
      List<Map<K, FileEntry>> removedSegments = new ArrayList<>(segments.size());
      long stamp = resizeLock.writeLock();
      try {
         for (int segment : segments) {
            if (entries[segment] == null)
               continue;

            removedSegments.add(entries[segment]);
            entries[segment] = null;
         }
      } finally {
         resizeLock.unlockWrite(stamp);
      }

      try {
         for (Map<K, FileEntry> removedSegment : removedSegments) {
            for (FileEntry fileEntry : removedSegment.values()) {
               free(fileEntry);
            }
         }
      } catch (IOException e) {
         throw new PersistenceException(e);
      }

      // Disk space optimizations
      processFreeEntries();
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
         return Long.compare(offset, fe.offset);
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
