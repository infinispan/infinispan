package org.infinispan.persistence.rocksdb;

import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.marshall.persistence.impl.MarshallableEntryImpl;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.internal.PersistenceUtil;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration;
import org.infinispan.persistence.rocksdb.logging.Log;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.MarshalledValue;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;

@ConfiguredBy(RocksDBStoreConfiguration.class)
public class RocksDBStore<K, V> implements NonBlockingStore<K, V> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   private static final byte[] BEGIN_KEY = createAndFillArray(1, (byte) 0x00);
   private static final byte[] END_KEY = createAndFillArray(128, (byte) 0xff);

   static final String DATABASE_PROPERTY_NAME_WITH_SUFFIX = "database.";
   static final String COLUMN_FAMILY_PROPERTY_NAME_WITH_SUFFIX = "data.";
   static final byte[] META_COLUMN_FAMILY = "meta-cf".getBytes();
   static final byte[] META_COLUMN_FAMILY_KEY = "metadata".getBytes();

   protected RocksDBStoreConfiguration configuration;
   private RocksDB db;
   private RocksDB expiredDb;
   private InitializationContext ctx;
   private TimeService timeService;
   private WriteOptions dataWriteOptions;
   private RocksDBHandler handler;
   private Properties databaseProperties;
   private Properties columnFamilyProperties;
   private Marshaller marshaller;
   private KeyPartitioner keyPartitioner;
   private MarshallableEntryFactory<K, V> entryFactory;
   private BlockingManager blockingManager;

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      this.configuration = ctx.getConfiguration();
      this.ctx = ctx;
      this.timeService = ctx.getTimeService();
      this.marshaller = ctx.getPersistenceMarshaller();
      this.entryFactory = ctx.getMarshallableEntryFactory();
      this.blockingManager = ctx.getBlockingManager();
      this.keyPartitioner = ctx.getKeyPartitioner();

      ctx.getPersistenceMarshaller().register(new PersistenceContextInitializerImpl());

      // Has to be done before we open the database, so we can pass the properties
      Properties allProperties = configuration.properties();
      for (Map.Entry<Object, Object> entry : allProperties.entrySet()) {
         String key = entry.getKey().toString();
         if (key.startsWith(DATABASE_PROPERTY_NAME_WITH_SUFFIX)) {
            if (databaseProperties == null) {
               databaseProperties = new Properties();
            }
            databaseProperties.setProperty(key.substring(DATABASE_PROPERTY_NAME_WITH_SUFFIX.length()), entry.getValue().toString());
         } else if (key.startsWith(COLUMN_FAMILY_PROPERTY_NAME_WITH_SUFFIX)) {
            if (columnFamilyProperties == null) {
               columnFamilyProperties = new Properties();
            }
            columnFamilyProperties.setProperty(key.substring(COLUMN_FAMILY_PROPERTY_NAME_WITH_SUFFIX.length()), entry.getValue().toString());
         }
      }

      return blockingManager.runBlocking(() -> {
         try {
            initDefaultHandler();
            MetadataImpl existingMeta = handler.loadMetadata();
            if (existingMeta == null && !configuration.purgeOnStartup()) {
               String cacheName = ctx.getCache().getName();
               // Metadata does not exist, therefore we must be reading from a pre-12.x store. Migrate the old data
               PERSISTENCE.startMigratingPersistenceData(cacheName);
               migrateFromV11();
               PERSISTENCE.persistedDataSuccessfulMigrated(cacheName);
            }
            // Update the metadata entry to use the current Infinispan version
            handler.writeMetadata();
         } catch (Exception e) {
            throw new CacheConfigurationException("Unable to open database", e);
         }
      }, "rocksdb-open");
   }

   private void initDefaultHandler() throws RocksDBException {
      this.handler = createHandler(getLocation(), getExpirationLocation());
      this.db = handler.db;
      this.expiredDb = handler.expiredDb;
   }

   private RocksDBHandler createHandler(Path data, Path expired) throws RocksDBException {
      AdvancedCache<?, ?> cache = ctx.getCache().getAdvancedCache();
      if (configuration.segmented()) {
         return new SegmentedRocksDBHandler(data, expired, cache.getCacheConfiguration().clustering().hash().numSegments());
      }
      return new NonSegmentedRocksDBHandler(data, expired, keyPartitioner);
   }

   @SuppressWarnings("checkstyle:ForbiddenMethod")
   private void migrateFromV11() throws IOException, RocksDBException {
      IntSet segments;
      if (configuration.segmented()) {
         int numSegments = ctx.getCache().getCacheConfiguration().clustering().hash().numSegments();
         segments = IntSets.immutableRangeSet(numSegments);
      } else {
         segments = null;
      }

      // If no entries exist in the store, then nothing to migrate
      if (CompletionStages.join(handler.size(segments)) == 0)
         return;

      Path newDbLocation = getQualifiedLocation("new_data");
      Path newExpiredDbLocation = getQualifiedLocation("new_expired");
      try {
         // Create new DB and open handle
         RocksDBHandler migrationHandler = createHandler(newDbLocation, newExpiredDbLocation);

         Function<RocksIterator, Flowable<MarshallableEntry<K, V>>> function =
               it -> Flowable.fromIterable(() -> new RocksLegacyEntryIterator(it));

         // Iterate and convert entries from old handle
         Publisher<MarshallableEntry<K, V>> publisher = configuration.segmented() ?
               ((SegmentedRocksDBHandler) handler).handleIteratorFunction(function, segments) :
               handler.publish(-1, function);

         WriteBatch batch = new WriteBatch();
         Set<MarshallableEntry<K, V>> expirableEntries = new HashSet<>();
         Flowable.fromPublisher(publisher)
               .blockingSubscribe(e -> {
                  ColumnFamilyHandle handle = migrationHandler.getHandle(keyPartitioner.getSegment(e.getKey()));
                  batch.put(handle, e.getKeyBytes().copy().getBuf(), marshall(e.getMarshalledValue()));
                  if (e.expiryTime() > 1)
                     expirableEntries.add(e);
               });

         if (batch.count() <= 0)
            batch.close();

         migrationHandler.db.write(dataWriteOptions(), batch);
         for (MarshallableEntry<K, V> e : expirableEntries)
            addNewExpiry(migrationHandler.expiredDb, e);

         // Close original and new handler
         handler.close();
         migrationHandler.close();

         // Copy new db to original location
         Path dataLocation = getLocation();
         Path expirationLocation = getExpirationLocation();
         Util.recursiveFileRemove(dataLocation);
         Util.recursiveFileRemove(expirationLocation);
         Files.move(newDbLocation, dataLocation, StandardCopyOption.REPLACE_EXISTING);
         Files.move(newExpiredDbLocation, expirationLocation, StandardCopyOption.REPLACE_EXISTING);

         // Open db handle to new db at original location
         initDefaultHandler();
      } finally {
         // In the event of a failure, always remove the new dbs
         Util.recursiveFileRemove(newDbLocation);
         Util.recursiveFileRemove(newExpiredDbLocation);
      }
   }

   private Path getQualifiedLocation(String qualifier) {
      return org.infinispan.persistence.PersistenceUtil.getQualifiedLocation(ctx.getGlobalConfiguration(), configuration.location(), ctx.getCache().getName(), qualifier);
   }

   private Path getLocation() {
      return getQualifiedLocation("data");
   }

   private Path getExpirationLocation() {
      return getQualifiedLocation("expired");
   }

   private WriteOptions dataWriteOptions() {
      if (dataWriteOptions == null)
         dataWriteOptions = new WriteOptions().setDisableWAL(false);
      return dataWriteOptions;
   }

   private DBOptions dataDbOptions() {
      DBOptions dbOptions;
      if (databaseProperties != null) {
         dbOptions = DBOptions.getDBOptionsFromProps(databaseProperties);
         if (dbOptions == null) {
            throw log.rocksDBUnknownPropertiesSupplied(databaseProperties.toString());
         }
      } else {
         dbOptions = new DBOptions();
      }
      return dbOptions
            .setCreateIfMissing(true)
            // We have to create missing column families on open.
            // Otherwise when we start we won't know what column families this database had if any - thus
            // we must specify all of them and later remove them.
            .setCreateMissingColumnFamilies(true);
   }

   protected Options expiredDbOptions() {
      return new Options()
            .setCreateIfMissing(true)
            // Make sure keys are sorted by bytes - we use this sorting to remove entries that have expired most recently
            .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
   }

   /**
    * Creates database if it doesn't exist.
    */
   protected static RocksDB openDatabase(Path location, Options options) throws RocksDBException {
      File dir = location.toFile();
      dir.mkdirs();
      return RocksDB.open(options, location.toString());
   }

   @Override
   public CompletionStage<Void> stop() {
      return blockingManager.runBlocking(() -> {
         // it could be null if an issue occurs during the initialization
         if (handler != null) {
            handler.close();
         }
      }, "rocksdb-stop");
   }

   @Override
   public Set<Characteristic> characteristics() {
      return EnumSet.of(Characteristic.BULK_READ, Characteristic.EXPIRATION, Characteristic.SEGMENTABLE);
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      return blockingManager.supplyBlocking(() -> getLocation().toFile().exists() && getExpirationLocation().toFile().exists(),
            "rocksdb-available");
   }

   @Override
   public CompletionStage<Void> clear() {
      return handler.clear();
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      return handler.size(segments);
   }

   @Override
   public CompletionStage<Long> approximateSize(IntSet segments) {
      return handler.approximateSize(segments);
   }

   @Override
   public CompletionStage<Boolean> containsKey(int segment, Object key) {
      // This might be able to use RocksDB#keyMayExist - but API is a bit flaky
      return load(segment, key)
            .thenApply(Objects::nonNull);
   }

   @Override
   public Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      return Flowable.fromPublisher(handler.publishEntries(segments, filter, false))
            .map(MarshallableEntry::getKey);
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean includeValues) {
      return handler.publishEntries(segments, filter, includeValues);
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      return handler.delete(segment, key);
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      return handler.write(segment, entry);
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      return handler.load(segment, key);
   }

   @Override
   public CompletionStage<Void> batch(int publisherCount, Publisher<SegmentedPublisher<Object>> removePublisher,
                                      Publisher<SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) {
      WriteBatch batch = new WriteBatch();
      Set<MarshallableEntry<K, V>> expirableEntries = new HashSet<>();
      Flowable.fromPublisher(removePublisher)
            .subscribe(sp -> {
               ColumnFamilyHandle handle = handler.getHandle(sp.getSegment());
               Flowable.fromPublisher(sp)
                     .subscribe(removed -> batch.delete(handle, marshall(removed)));
            });
      Flowable.fromPublisher(writePublisher)
            .subscribe(sp -> {
               ColumnFamilyHandle handle = handler.getHandle(sp.getSegment());
               Flowable.fromPublisher(sp)
                     .subscribe(me -> {
                        batch.put(handle, marshall(me.getKey()), marshall(me.getMarshalledValue()));
                        if (me.expiryTime() > -1) {
                           expirableEntries.add(me);
                        }
                     });
            });
      if (batch.count() <= 0) {
         batch.close();
         return CompletableFutures.completedNull();
      }
      return blockingManager.runBlocking(() -> {
         try {
            db.write(dataWriteOptions(), batch);
            for (MarshallableEntry<K, V> me : expirableEntries) {
               addNewExpiry(expiredDb, me);
            }
         } catch (RocksDBException e) {
            throw new PersistenceException(e);
         }
      }, "rocksdb-batch").whenComplete((ignore, t) -> batch.close());
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> purgeExpired() {
      Publisher<List<MarshallableEntry<K, V>>> purgedBatches = blockingManager.blockingPublisher(Flowable.defer(() -> {
         // We check expiration based on time of subscription only
         long now = timeService.wallClockTime();
         return actualPurgeExpired(now)
               // We return a buffer of expired entries emitted to the non blocking thread
               // This prevents waking up the non blocking thread for every entry as they will most likely be
               // consumed much faster than emission (since each emission performs a get and remove)
               .buffer(16);
      }));

      return Flowable.fromPublisher(purgedBatches)
            .concatMap(Flowable::fromIterable);
   }

   private Flowable<MarshallableEntry<K, V>> actualPurgeExpired(long now) {
      // The following flowable is responsible for emitting entries that have expired from expiredDb and removing the
      // given entries
      Flowable<byte[]> expiredFlowable = Flowable.using(() -> {
         ReadOptions readOptions = new ReadOptions().setFillCache(false);
         return new AbstractMap.SimpleImmutableEntry<>(readOptions, expiredDb.newIterator(readOptions));
      }, entry -> {
         if (entry.getValue() == null) {
            return Flowable.empty();
         }
         RocksIterator iterator = entry.getValue();
         iterator.seekToFirst();

         return Flowable.fromIterable(() ->
               new BaseRocksIterator<>(iterator) {
                  @Override
                  protected byte[] getNext() {
                     byte[] keyBytes = readKey();
                     if (keyBytes == null)
                        return null;

                     Long time = unmarshall(keyBytes);
                     if (time > now)
                        return null;
                     try {
                        expiredDb.delete(keyBytes);
                     } catch (RocksDBException e) {
                        throw new PersistenceException(e);
                     }
                     byte[] value = readValue();
                     moveNext();
                     return value;
                  }


               });
      }, entry -> {
         entry.getKey().close();
         RocksIterator rocksIterator = entry.getValue();
         if (rocksIterator != null) {
            synchronized (rocksIterator) {
               rocksIterator.close();
            }
         }
      });

      Flowable<MarshallableEntry<K, V>> expiredEntryFlowable = expiredFlowable.flatMap(expiredBytes -> {
         Object bucketKey = unmarshall(expiredBytes);
         if (bucketKey instanceof ExpiryBucket) {
            return Flowable.fromIterable(((ExpiryBucket) bucketKey).entries)
                  .flatMapMaybe(marshalledKey -> {
                     ColumnFamilyHandle columnFamilyHandle = handler.getHandleForMarshalledKey(marshalledKey);
                     MarshalledValue mv = handlePossiblyExpiredKey(columnFamilyHandle, marshalledKey, now);
                     return mv == null ? Maybe.empty() : Maybe.just(entryFactory.create(unmarshall(marshalledKey), mv));
                  });
         } else {
            // The bucketKey is an actual key
            ColumnFamilyHandle columnFamilyHandle = handler.getHandle(bucketKey);
            MarshalledValue mv = handlePossiblyExpiredKey(columnFamilyHandle, marshall(bucketKey), now);
            return mv == null ? Flowable.empty() : Flowable.just(entryFactory.create(bucketKey, mv));
         }
      });

      if (log.isTraceEnabled()) {
         // Note this tracing only works properly for one subscriber
         FlowableProcessor<MarshallableEntry<K, V>> mirrorEntries = UnicastProcessor.create();
         expiredEntryFlowable = expiredEntryFlowable
               .doOnEach(mirrorEntries)
               .doOnSubscribe(subscription -> log.tracef("Purging entries from RocksDBStore"));
         mirrorEntries.count()
               .subscribe(count -> log.tracef("Purged %d entries from RocksDBStore", count));
      }

      return expiredEntryFlowable;
   }

   private MarshalledValue handlePossiblyExpiredKey(ColumnFamilyHandle columnFamilyHandle, byte[] marshalledKey,
                                                    long now) throws RocksDBException {
      byte[] valueBytes = db.get(columnFamilyHandle, marshalledKey);
      if (valueBytes == null) {
         return null;
      }
      MarshalledValue mv = unmarshall(valueBytes);
      if (mv != null) {
         // TODO race condition: the entry could be updated between the get and delete!
         Metadata metadata = unmarshall(MarshallUtil.toByteArray(mv.getMetadataBytes()));
         if (MarshallableEntryImpl.isExpired(metadata, now, mv.getCreated(), mv.getLastUsed())) {
            // somewhat inefficient to FIND then REMOVE... but required if the value is updated
            db.delete(columnFamilyHandle, marshalledKey);
            return mv;
         }
      }
      return null;
   }

   @Override
   public CompletionStage<Void> addSegments(IntSet segments) {
      return handler.addSegments(segments);
   }

   @Override
   public CompletionStage<Void> removeSegments(IntSet segments) {
      return handler.removeSegments(segments);
   }

   private byte[] marshall(Object entry) {
      try {
         return marshaller.objectToByteBuffer(entry);
      } catch (IOException e) {
         throw new PersistenceException(e);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new PersistenceException(e);
      }
   }

   private <E> E unmarshall(byte[] bytes, Marshaller marshaller) {
      if (bytes == null)
         return null;

      try {
         //noinspection unchecked
         return (E) marshaller.objectFromByteBuffer(bytes);
      } catch (IOException | ClassNotFoundException e) {
         throw new PersistenceException(e);
      }
   }

   private <E> E unmarshall(byte[] bytes) {
      return unmarshall(bytes, this.marshaller);
   }

   private MarshallableEntry<K, V> unmarshallEntry(Object key, byte[] valueBytes) {
      MarshalledValue value = unmarshall(valueBytes);
      if (value == null) return null;

      return entryFactory.create(key, value.getValueBytes(), value.getMetadataBytes(), value.getInternalMetadataBytes(),
            value.getCreated(), value.getLastUsed());
   }

   private void addNewExpiry(RocksDB expiredDb, MarshallableEntry<? extends K, ? extends V> entry) throws RocksDBException {
      long expiry = entry.expiryTime();
      long maxIdle = entry.getMetadata().maxIdle();
      if (maxIdle > 0) {
         // Coding getExpiryTime() for transient entries has the risk of being a moving target
         // which could lead to unexpected results, hence, InternalCacheEntry calls are required
         expiry = maxIdle + ctx.getTimeService().wallClockTime();
      }
      byte[] keyBytes = entry.getKeyBytes().copy().getBuf();
      putExpireDbData(expiredDb, new ExpiryEntry(expiry, keyBytes));
   }

   @ProtoTypeId(ProtoStreamTypeIds.ROCKSDB_EXPIRY_BUCKET)
   static final class ExpiryBucket {
      @ProtoField(number = 1, collectionImplementation = ArrayList.class)
      List<byte[]> entries;

      ExpiryBucket() {
      }

      ExpiryBucket(byte[] existingKey, byte[] newKey) {
         entries = new ArrayList<>(2);
         entries.add(existingKey);
         entries.add(newKey);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.ROCKSDB_PERSISTED_METADATA)
   static final class MetadataImpl {
      @ProtoField(number = 1, defaultValue = "-1")
      short version;

      @ProtoFactory
      MetadataImpl(short version) {
         this.version = version;
      }
   }

   private static final class ExpiryEntry {

      final long expiry;
      final byte[] keyBytes;

      ExpiryEntry(long expiry, byte[] keyBytes) {
         this.expiry = expiry;
         this.keyBytes = keyBytes;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ExpiryEntry that = (ExpiryEntry) o;
         return expiry == that.expiry &&
               Arrays.equals(keyBytes, that.keyBytes);
      }

      @Override
      public int hashCode() {
         int result = Objects.hash(expiry);
         result = 31 * result + Arrays.hashCode(keyBytes);
         return result;
      }
   }

   private class RocksLegacyEntryIterator extends BaseRocksIterator<MarshallableEntry<K, V>> {
      private final long now;
      private final PersistenceMarshaller pm;
      private final Marshaller userMarshaller;

      RocksLegacyEntryIterator(RocksIterator it) {
         super(it);
         this.now = timeService.wallClockTime();
         this.pm = ctx.getPersistenceMarshaller();
         this.userMarshaller = pm.getUserMarshaller();
      }

      @Override
      protected MarshallableEntry<K, V> getNext() {
         MarshallableEntry<K, V> entry = null;
         while (entry == null) {
            K key = unmarshall(readKey(), userMarshaller);
            if (key == null) break;

            MarshalledValue mv = unmarshall(readValue(), pm);
            if (mv == null) break;
            V value = unmarshall(mv.getValueBytes().getBuf(), userMarshaller);
            Metadata meta;
            try {
               meta = unmarshall(mv.getMetadataBytes().getBuf(), userMarshaller);
            } catch (IllegalArgumentException e) {
               // For metadata we need to attempt to read with user-marshaller first in case custom metadata used, otherwise use the persistence marshaller
               meta = unmarshall(mv.getMetadataBytes().getBuf(), pm);
            }

            PrivateMetadata internalMeta = unmarshall(mv.getInternalMetadataBytes().copy().getBuf(), userMarshaller);
            MarshallableEntry<K, V> me = entryFactory.create(key, value, meta, internalMeta, mv.getCreated(), mv.getLastUsed());
            if (me != null && !me.isExpired(now)) {
               entry = me;
            }
            moveNext();
         }
         return entry;
      }
   }

   private abstract static class BaseRocksIterator<T> extends AbstractIterator<T> {
      private final RocksIterator it;

      private BaseRocksIterator(RocksIterator it) {
         this.it = it;
      }

      protected boolean isValid() {
         synchronized (it) {
            return it.isOwningHandle() && it.isValid();
         }
      }

      protected byte[] readKey() {
         synchronized (it) {
            if (!isValid()) return null;

            return it.key();
         }
      }

      protected byte[] readValue() {
         synchronized (it) {
            if (!isValid()) return null;
            return it.value();
         }
      }

      protected void moveNext() {
         synchronized (it) {
            if (isValid()) {
               it.next();
            }
         }
      }
   }

   private class RocksEntryIterator extends BaseRocksIterator<MarshallableEntry<K, V>> {
      private final Predicate<? super K> filter;
      private final long now;

      RocksEntryIterator(RocksIterator it, Predicate<? super K> filter, long now) {
         super(it);
         this.filter = filter;
         this.now = now;
      }

      @Override
      protected MarshallableEntry<K, V> getNext() {
         MarshallableEntry<K, V> entry = null;
         while (entry == null) {
            K key = unmarshall(readKey());
            if (key == null) break;
            if (filter == null || filter.test(key)) {
               MarshallableEntry<K, V> me = unmarshallEntry(key, readValue());
               if (me != null && !me.isExpired(now)) {
                  entry = me;
               }
            }
            moveNext();
         }
         return entry;
      }
   }

   private abstract class RocksDBHandler {

      protected RocksDB db;
      protected RocksDB expiredDb;
      protected ColumnFamilyHandle metaColumnFamilyHandle;

      abstract RocksDB open(Path location, DBOptions options) throws RocksDBException;

      abstract void close();

      abstract ColumnFamilyHandle getHandle(int segment);

      abstract ColumnFamilyHandle getHandle(Object key);

      abstract ColumnFamilyHandle getHandleForMarshalledKey(byte[] marshalledKey);

      void writeMetadata() throws RocksDBException {
         MetadataImpl metadata = new MetadataImpl(Version.getVersionShort());
         db.put(metaColumnFamilyHandle, META_COLUMN_FAMILY_KEY, marshall(metadata));
      }

      MetadataImpl loadMetadata() throws RocksDBException {
         return unmarshall(db.get(metaColumnFamilyHandle, META_COLUMN_FAMILY_KEY));
      }

      ColumnFamilyDescriptor newDescriptor(byte[] name) {
         ColumnFamilyOptions columnFamilyOptions;
         if (columnFamilyProperties != null) {
            columnFamilyOptions = ColumnFamilyOptions.getColumnFamilyOptionsFromProps(columnFamilyProperties);
            if (columnFamilyOptions == null) {
               throw log.rocksDBUnknownPropertiesSupplied(columnFamilyProperties.toString());
            }
         } else {
            columnFamilyOptions = new ColumnFamilyOptions();
         }
         if (configuration.attributes().attribute(RocksDBStoreConfiguration.COMPRESSION_TYPE).isModified()) {
            columnFamilyOptions.setCompressionType(configuration.compressionType().getValue());
         }
         return new ColumnFamilyDescriptor(name, columnFamilyOptions);
      }

      CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
         ColumnFamilyHandle handle = getHandle(segment);
         if (handle == null) {
            log.trace("Ignoring load as handle is not currently configured");
            return CompletableFutures.completedNull();
         }
         try {
            CompletionStage<byte[]> entryByteStage = blockingManager.supplyBlocking(() -> {
               try {
                  return db.get(handle, marshall(key));
               } catch (RocksDBException e) {
                  throw new CompletionException(e);
               }
            }, "rocksdb-load");
            return entryByteStage.thenApply(entryBytes -> {
               MarshallableEntry<K, V> me = unmarshallEntry(key, entryBytes);
               if (me == null || me.isExpired(timeService.wallClockTime())) {
                  return null;
               }
               return me;
            });
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }

      CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> me) {
         ColumnFamilyHandle handle = getHandle(segment);
         if (handle == null) {
            log.trace("Ignoring write as handle is not currently configured");
            return CompletableFutures.completedNull();
         }
         try {
            byte[] marshalledKey = MarshallUtil.toByteArray(me.getKeyBytes());
            byte[] marshalledValue = marshall(me.getMarshalledValue());
            return blockingManager.runBlocking(() -> {
               try {
                  db.put(handle, marshalledKey, marshalledValue);
                  if (me.expiryTime() > -1) {
                     addNewExpiry(expiredDb, me);
                  }
               } catch (RocksDBException e) {
                  throw new PersistenceException(e);
               }
            }, "rocksdb-write");

         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }

      CompletionStage<Boolean> delete(int segment, Object key) {
         try {
            byte[] keyBytes = marshall(key);
            ColumnFamilyHandle handle = getHandle(segment);
            return blockingManager.supplyBlocking(() -> {
               try {
                  db.delete(handle, keyBytes);
                  return null;
               } catch (RocksDBException e) {
                  throw new PersistenceException(e);
               }
            }, "rocksdb-delete");
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }

      abstract CompletionStage<Void> clear();

      abstract Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter,
                                                                 boolean fetchValue);

      CompletionStage<Long> size(IntSet segments) {
         return Flowable.fromPublisher(publishKeys(segments, null))
               .count().toCompletionStage();
      }

      abstract CompletionStage<Long> approximateSize(IntSet segments);

      <P> Publisher<P> publish(int segment, Function<RocksIterator, Flowable<P>> function) {
         ReadOptions readOptions = new ReadOptions().setFillCache(false);
         return blockingManager.blockingPublisher(Flowable.using(() -> wrapIterator(db, readOptions, segment), iterator -> {
            if (iterator == null) {
               return Flowable.empty();
            }
            // Called first, no need for synchronization.
            iterator.seekToFirst();
            return function.apply(iterator);
         }, iterator -> {
            if (iterator != null) {
               synchronized (iterator) {
                  iterator.close();
               }
            }
            readOptions.close();
         }));
      }

      abstract RocksIterator wrapIterator(RocksDB db, ReadOptions readOptions, int segment);

      abstract CompletionStage<Void> addSegments(IntSet segments);

      abstract CompletionStage<Void> removeSegments(IntSet segments);
   }

   private final class NonSegmentedRocksDBHandler extends RocksDBHandler {
      private final KeyPartitioner keyPartitioner;

      private ColumnFamilyHandle defaultColumnFamilyHandle;

      private NonSegmentedRocksDBHandler(Path data, Path expired, KeyPartitioner keyPartitioner) throws RocksDBException {
         this.db = open(data, dataDbOptions());
         this.expiredDb = openDatabase(expired, expiredDbOptions());
         this.keyPartitioner = keyPartitioner;
      }

      @Override
      ColumnFamilyHandle getHandle(int segment) {
         return defaultColumnFamilyHandle;
      }

      @Override
      ColumnFamilyHandle getHandle(Object key) {
         return defaultColumnFamilyHandle;
      }

      @Override
      ColumnFamilyHandle getHandleForMarshalledKey(byte[] marshalledKey) {
         return defaultColumnFamilyHandle;
      }

      @Override
      RocksDB open(Path location, DBOptions options) throws RocksDBException {
         File dir = location.toFile();
         dir.mkdirs();
         List<ColumnFamilyDescriptor> descriptors = new ArrayList<>(2);
         List<ColumnFamilyHandle> handles = new ArrayList<>(2);
         descriptors.add(newDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
         descriptors.add(newDescriptor(META_COLUMN_FAMILY));
         RocksDB rocksDB = RocksDB.open(options, location.toString(), descriptors, handles);

         defaultColumnFamilyHandle = handles.get(0);
         metaColumnFamilyHandle = handles.get(1);
         return rocksDB;
      }

      @Override
      CompletionStage<Void> clear() {
         return clear(null);
      }

      CompletionStage<Void> clear(IntSet segments) {
         return blockingManager.runBlocking(() -> {
            if (segments == null) {
               clearColumnFamily(defaultColumnFamilyHandle);
            } else {
               try (ReadOptions readOptions = new ReadOptions().setFillCache(false)) {
                  try (RocksIterator it = db.newIterator(defaultColumnFamilyHandle, readOptions)) {
                     for (it.seekToFirst(); it.isValid(); it.next()) {
                        byte[] keyBytes = it.key();
                        Object key = unmarshall(keyBytes);
                        int segment = keyPartitioner.getSegment(key);
                        if (segments.contains(segment)) {
                           db.delete(defaultColumnFamilyHandle, keyBytes);
                        }
                     }
                  }
               } catch (Exception e) {
                  throw new PersistenceException(e);
               }
            }
         }, "rocksdb-clear");
      }

      @Override
      void close() {
         defaultColumnFamilyHandle.close();

         db.close();
         expiredDb.close();
      }

      protected RocksIterator wrapIterator(RocksDB db, ReadOptions readOptions, int segment) {
         return db.newIterator(defaultColumnFamilyHandle, readOptions);
      }

      @Override
      Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue) {
         Predicate<? super K> combinedFilter = PersistenceUtil.combinePredicate(segments, keyPartitioner, filter);
         return publish(-1, it -> Flowable.fromIterable(() -> {
            // Make sure this is taken when the iterator is created
            long now = timeService.wallClockTime();
            return new RocksEntryIterator(it, combinedFilter, now);
         }));
      }

      @Override
      CompletionStage<Void> addSegments(IntSet segments) {
         // Do nothing
         return CompletableFutures.completedNull();
      }

      @Override
      CompletionStage<Void> removeSegments(IntSet segments) {
         // Unfortunately we have to clear all entries that map to each entry, which requires a full iteration and
         // segment check on every entry
         return clear(segments);
      }

      @Override
      CompletionStage<Long> approximateSize(IntSet segments) {
         return blockingManager.supplyBlocking(() -> {
            try {
               return Long.parseLong(db.getProperty(defaultColumnFamilyHandle, "rocksdb.estimate-num-keys"));
            } catch (RocksDBException e) {
               throw new PersistenceException(e);
            }
         }, "rocksdb-approximateSize");
      }
   }

   private class SegmentedRocksDBHandler extends RocksDBHandler {
      private final AtomicReferenceArray<ColumnFamilyHandle> handles;

      private SegmentedRocksDBHandler(Path data, Path expired, int segmentCount) throws RocksDBException {
         this.handles = new AtomicReferenceArray<>(segmentCount);
         this.db = open(data, dataDbOptions());
         this.expiredDb = openDatabase(expired, expiredDbOptions());
      }

      byte[] byteArrayFromInt(int val) {
         return new byte[]{
               (byte) (val >>> 24),
               (byte) (val >>> 16),
               (byte) (val >>> 8),
               (byte) (val)
         };
      }

      @Override
      ColumnFamilyHandle getHandle(int segment) {
         return handles.get(segment);
      }

      @Override
      ColumnFamilyHandle getHandle(Object key) {
         return handles.get(keyPartitioner.getSegment(key));
      }

      @Override
      ColumnFamilyHandle getHandleForMarshalledKey(byte[] marshalledKey) {
         return getHandle(unmarshall(marshalledKey));
      }

      @Override
      RocksDB open(Path location, DBOptions options) throws RocksDBException {
         File dir = location.toFile();
         dir.mkdirs();
         int segmentCount = handles.length();
         List<ColumnFamilyDescriptor> descriptors = new ArrayList<>(segmentCount + 2);
         List<ColumnFamilyHandle> outHandles = new ArrayList<>(segmentCount + 2);
         // You have to open the default column family
         descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));

         // Create the meta column family
         descriptors.add(new ColumnFamilyDescriptor(META_COLUMN_FAMILY, new ColumnFamilyOptions()));

         for (int i = 0; i < segmentCount; ++i) {
            descriptors.add(newDescriptor(byteArrayFromInt(i)));
         }

         RocksDB rocksDB = RocksDB.open(options, location.toString(), descriptors, outHandles);
         metaColumnFamilyHandle = outHandles.get(1);
         for (int i = 0; i < segmentCount; ++i) {
            handles.set(i, outHandles.get(i + 2));
         }
         return rocksDB;
      }

      @Override
      CompletionStage<Void> clear() {
         return blockingManager.runBlocking(() -> {
            for (int i = 0; i < handles.length(); ++i) {
               clearForSegment(i);
            }
         }, "rocksdb-clear");
      }

      /**
       * Clear out the entries for a segment
       *
       * @param segment the segment to clear out
       */
      private void clearForSegment(int segment) {
         ColumnFamilyHandle handle = handles.get(segment);
         RocksDBStore.this.clearColumnFamily(handle);
      }

      @Override
      void close() {
         for (int i = 0; i < handles.length(); ++i) {
            ColumnFamilyHandle handle = handles.getAndSet(i, null);
            if (handle != null) {
               handle.close();
            }
         }

         db.close();
         expiredDb.close();
      }

      @Override
      Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue) {
         Function<RocksIterator, Flowable<MarshallableEntry<K, V>>> function = it -> Flowable.fromIterable(() -> {
            long now = timeService.wallClockTime();
            return new RocksEntryIterator(it, filter, now);
         });
         return handleIteratorFunction(function, segments);
      }

      <R> Publisher<R> handleIteratorFunction(Function<RocksIterator, Flowable<R>> function, IntSet segments) {
         // Short circuit if only a single segment - assumed to be invoked from persistence thread
         if (segments != null && segments.size() == 1) {
            return publish(segments.iterator().nextInt(), function);
         }
         IntSet segmentsToUse = segments == null ? IntSets.immutableRangeSet(handles.length()) : segments;
         return Flowable.fromStream(segmentsToUse.intStream().mapToObj(i -> publish(i, function)))
               .concatMap(RxJavaInterop.identityFunction());
      }

      @Override
      RocksIterator wrapIterator(RocksDB db, ReadOptions readOptions, int segment) {
         ColumnFamilyHandle handle = handles.get(segment);
         if (handle != null) {
            return db.newIterator(handle, readOptions);
         }
         return null;
      }

      @Override
      CompletionStage<Void> addSegments(IntSet segments) {
         Flowable<Integer> segmentFlowable = Flowable.fromIterable(segments)
               .filter(segment -> handles.get(segment) == null);

         return blockingManager.subscribeBlockingConsumer(segmentFlowable, segment -> {
            if (log.isTraceEnabled()) {
               log.tracef("Creating column family for segment %d", segment);
            }
            byte[] cfName = byteArrayFromInt(segment);
            try {
               ColumnFamilyHandle handle = db.createColumnFamily(newDescriptor(cfName));
               handles.set(segment, handle);
            } catch (RocksDBException e) {
               throw new PersistenceException(e);
            }
         }, "testng-addSegments");
      }

      @Override
      CompletionStage<Void> removeSegments(IntSet segments) {
         Flowable<ColumnFamilyHandle> handleFlowable = Flowable.fromIterable(segments)
               .map(segment -> {
                  ColumnFamilyHandle cf = handles.getAndSet(segment, null);
                  return cf != null ? cf : this;
               }).ofType(ColumnFamilyHandle.class);

         return blockingManager.subscribeBlockingConsumer(handleFlowable, handle -> {
            if (log.isTraceEnabled()) {
               log.tracef("Dropping column family %s", handle);
            }
            try {
               db.dropColumnFamily(handle);
            } catch (RocksDBException e) {
               throw new PersistenceException(e);
            }
            handle.close();
         }, "testng-removeSegments");
      }

      @Override
      CompletionStage<Long> approximateSize(IntSet segments) {
         return blockingManager.supplyBlocking(() -> {
            long size = 0;
            for (int segment : segments) {
               ColumnFamilyHandle handle = getHandle(segment);
               try {
                  size += Long.parseLong(db.getProperty(handle, "rocksdb.estimate-num-keys"));
               } catch (RocksDBException e) {
                  throw new PersistenceException(e);
               }
            }
            return size;
         }, "rocksdb-approximateSize");
      }
   }

   private void putExpireDbData(RocksDB expiredDb, ExpiryEntry entry) throws RocksDBException {
      final byte[] expiryBytes = marshall(entry.expiry);
      final byte[] existingBytes = expiredDb.get(expiryBytes);

      if (existingBytes != null) {
         // in the case of collision make the value a List ...
         final Object existing = unmarshall(existingBytes);
         if (existing instanceof ExpiryBucket) {
            ((ExpiryBucket) existing).entries.add(entry.keyBytes);
            expiredDb.put(expiryBytes, marshall(existing));
         } else {
            ExpiryBucket bucket = new ExpiryBucket(existingBytes, entry.keyBytes);
            expiredDb.put(expiryBytes, marshall(bucket));
         }
      } else {
         expiredDb.put(expiryBytes, entry.keyBytes);
      }
   }

   /*
    * Instead of iterate in RocksIterator we use the first and last byte array
    */
   private void clearColumnFamily(ColumnFamilyHandle handle) {
      try {
         // when the data under a segment was removed, the handle will be null
         if (handle != null) {
            db.deleteRange(handle, BEGIN_KEY, END_KEY);
            // We don't control the keys, the marshaller does.
            // In theory it is possible that a custom marshaller would generate a key of 10k 0xff bytes
            // That key would be after END_KEY, so the deleteRange call wouldn't remove it
            // If there are remaining keys, remove.
            try (ReadOptions iteratorOptions = new ReadOptions().setFillCache(false)) {
               try (RocksIterator it = db.newIterator(handle, iteratorOptions)) {
                  for (it.seekToFirst(); it.isValid(); it.next()) {
                     db.delete(handle, it.key());
                  }
               }
            }
         }
      } catch (RocksDBException e) {
         throw new PersistenceException(e);
      }
   }

   private static byte[] createAndFillArray(int length, byte value) {
      byte[] array = new byte[length];
      Arrays.fill(array, value);
      return array;
   }
}
