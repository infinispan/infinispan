package org.infinispan.persistence.rocksdb;

import static org.infinispan.persistence.PersistenceUtil.getQualifiedLocation;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.persistence.impl.MarshallableEntryImpl;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.internal.PersistenceUtil;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration;
import org.infinispan.persistence.rocksdb.logging.Log;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.MarshalledValue;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletableFutures;
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
   private static final boolean trace = log.isTraceEnabled();

   static final String DATABASE_PROPERTY_NAME_WITH_SUFFIX = "database.";
   static final String COLUMN_FAMILY_PROPERTY_NAME_WITH_SUFFIX = "data.";

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

      AdvancedCache cache = ctx.getCache().getAdvancedCache();
      if (configuration.segmented()) {
         handler = new SegmentedRocksDBHandler(cache.getCacheConfiguration().clustering().hash().numSegments());
      } else {
         handler = new NonSegmentedRocksDBHandler(keyPartitioner);
      }

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
            db = handler.open(getLocation(), dataDbOptions());
            expiredDb = openDatabase(getExpirationLocation(), expiredDbOptions());
         } catch (Exception e) {
            throw new CacheConfigurationException("Unable to open database", e);
         }
      }, "rocksdb-open");
   }

   private Path getLocation() {
      return getQualifiedLocation(ctx.getGlobalConfiguration(), configuration.location(), ctx.getCache().getName(), "data");
   }

   private Path getExpirationLocation() {
      return getQualifiedLocation(ctx.getGlobalConfiguration(), configuration.expiredLocation(), ctx.getCache().getName(), "expired");
   }

   private WriteOptions dataWriteOptions() {
      if (dataWriteOptions == null)
         dataWriteOptions = new WriteOptions().setDisableWAL(false);
      return dataWriteOptions;
   }

   protected DBOptions dataDbOptions() {
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
   protected RocksDB openDatabase(Path location, Options options) throws RocksDBException {
      File dir = location.toFile();
      dir.mkdirs();
      return RocksDB.open(options, location.toString());
   }

   @Override
   public CompletionStage<Void> stop() {
      return blockingManager.runBlocking(() -> {
         handler.close();
         expiredDb.close();
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
               addNewExpiry(me);
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
      Flowable<byte[]> expiredFlowable = Flowable.generate(() -> {
         ReadOptions readOptions = new ReadOptions().setFillCache(false);
         RocksIterator iterator = expiredDb.newIterator(readOptions);
         iterator.seekToFirst();
         return new AbstractMap.SimpleImmutableEntry<>(readOptions, iterator);
      }, (state, emitter) -> {
         RocksIterator iterator = state.getValue();
         while (iterator.isValid()) {
            byte[] keyBytes = iterator.key();
            Long time = unmarshall(keyBytes);
            if (time <= now) {
               // Found an expired entry
               // Delete the timestamp from the expiration DB
               try {
                  expiredDb.delete(keyBytes);
               } catch (RocksDBException e) {
                  throw new PersistenceException(e);
               }
               // Emit the bucket of expired keys and return
               byte[] value = iterator.value();
               emitter.onNext(value);
               iterator.next();
               return;
            }

            iterator.next();
         }

         // Got to the end of the iteration
         emitter.onComplete();
      }, entry -> {
         entry.getKey().close();
         RocksIterator rocksIterator = entry.getValue();
         if (rocksIterator != null) {
            rocksIterator.close();
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

      if (trace) {
         // Note this tracing only works properly for one subscriber
         FlowableProcessor<MarshallableEntry<K, V>> mirrorEntries = UnicastProcessor.create();
         expiredEntryFlowable = expiredEntryFlowable
               .doOnEach(mirrorEntries)
               .doOnSubscribe(subscription -> log.tracef("Purging entries from RocksDBStore"));
         mirrorEntries.count()
               .subscribe(count -> log.tracef("Purged %d entries from RocksDBStore"));
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

   private <E> E unmarshall(byte[] bytes) {
      if (bytes == null)
         return null;

      try {
         //noinspection unchecked
         return (E) marshaller.objectFromByteBuffer(bytes);
      } catch (IOException | ClassNotFoundException e) {
         throw new PersistenceException(e);
      }
   }

   private MarshallableEntry<K, V> unmarshallEntry(Object key, byte[] valueBytes) {
      MarshalledValue value = unmarshall(valueBytes);
      if (value == null) return null;

      return entryFactory.create(key, value.getValueBytes(), value.getMetadataBytes(), value.getInternalMetadataBytes(),
            value.getCreated(), value.getLastUsed());
   }

   private void addNewExpiry(MarshallableEntry entry) throws RocksDBException {
      long expiry = entry.expiryTime();
      long maxIdle = entry.getMetadata().maxIdle();
      if (maxIdle > 0) {
         // Coding getExpiryTime() for transient entries has the risk of being a moving target
         // which could lead to unexpected results, hence, InternalCacheEntry calls are required
         expiry = maxIdle + ctx.getTimeService().wallClockTime();
      }
      byte[] keyBytes = entry.getKeyBytes().copy().getBuf();
      putExpireDbData(new ExpiryEntry(expiry, keyBytes));
   }

   @ProtoTypeId(ProtoStreamTypeIds.ROCKSDB_EXPIRY_BUCKET)
   static final class ExpiryBucket {
      @ProtoField(number = 1, collectionImplementation = ArrayList.class)
      List<byte[]> entries;

      ExpiryBucket(){}

      ExpiryBucket(byte[] existingKey, byte[] newKey) {
         entries = new ArrayList<>(2);
         entries.add(existingKey);
         entries.add(newKey);
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

   private abstract class RocksDBHandler {

      abstract RocksDB open(Path location, DBOptions options) throws RocksDBException;

      abstract void close();

      abstract ColumnFamilyHandle getHandle(int segment);

      abstract ColumnFamilyHandle getHandle(Object key);

      abstract ColumnFamilyHandle getHandleForMarshalledKey(byte[] marshalledKey);

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
                     addNewExpiry(me);
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
                  if (db.get(handle, keyBytes) == null) {
                     return Boolean.FALSE;
                  }
                  db.delete(handle, keyBytes);
                  return Boolean.TRUE;
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

      Publisher<MarshallableEntry<K, V>> publish(int segment, Predicate<? super K> filter) {
         long now = timeService.wallClockTime();
         ReadOptions readOptions = new ReadOptions().setFillCache(false);
         return blockingManager.blockingPublisher(Flowable.generate(() -> {
                    RocksIterator iterator = wrapIterator(db, readOptions, segment);
                    iterator.seekToFirst();
                    return iterator;
                 },
                 (iterator, emitter) -> {
                    MarshallableEntry<K, V> entry = nextEntry(filter, now, iterator);
                    if (entry != null) {
                       emitter.onNext(entry);
                    }
                    if (!iterator.isValid()) {
                       emitter.onComplete();
                    }
                 },
                 iterator -> {
                    iterator.close();
                    readOptions.close();
                 }));
      }

      private MarshallableEntry<K, V> nextEntry(Predicate<? super K> filter, long now, RocksIterator iterator) {
         MarshallableEntry<K, V> entry = null;
         while (entry == null && iterator.isValid()) {
            K key = unmarshall(iterator.key());
            if (filter == null || filter.test(key)) {
               MarshallableEntry<K, V> me = unmarshallEntry(key, iterator.value());
               if (me != null && !me.isExpired(now)) {
                  entry = me;
               }
            }
            iterator.next();
         }
         return entry;
      }

      abstract RocksIterator wrapIterator(RocksDB db, ReadOptions readOptions, int segment);

      abstract CompletionStage<Void> addSegments(IntSet segments);

      abstract CompletionStage<Void> removeSegments(IntSet segments);
   }

   private final class NonSegmentedRocksDBHandler extends RocksDBHandler {
      private final KeyPartitioner keyPartitioner;

      private ColumnFamilyHandle defaultColumnFamilyHandle;

      private NonSegmentedRocksDBHandler(KeyPartitioner keyPartitioner) {
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
         List<ColumnFamilyHandle> handles = new ArrayList<>(1);
         ColumnFamilyDescriptor desc = newDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
         RocksDB rocksDB = RocksDB.open(options, location.toString(),
               Collections.singletonList(desc),
               handles);
         defaultColumnFamilyHandle = handles.get(0);
         return rocksDB;
      }

      @Override
      CompletionStage<Void> clear() {
         return clear(null);
      }

      CompletionStage<Void> clear(IntSet segments) {
         return blockingManager.runBlocking(() -> {
            long count = 0;
            boolean destroyDatabase = false;
            try (ReadOptions readOptions = new ReadOptions().setFillCache(false)) {
               RocksIterator optionalIterator = wrapIterator(db, readOptions, -1);
               if (optionalIterator != null && (configuration.clearThreshold() > 0 || segments == null)) {
                  try (RocksIterator it = optionalIterator) {
                     for (it.seekToFirst(); it.isValid(); it.next()) {
                        byte[] keyBytes = it.key();
                        if (segments != null) {
                           Object key = unmarshall(keyBytes);
                           int segment = keyPartitioner.getSegment(key);
                           if (segments.contains(segment)) {
                              db.delete(defaultColumnFamilyHandle, keyBytes);
                           }
                        } else {
                           db.delete(defaultColumnFamilyHandle, keyBytes);
                           count++;

                           if (count > configuration.clearThreshold()) {
                              destroyDatabase = true;
                              break;
                           }
                        }
                     }
                  } catch (RocksDBException e) {
                     if (segments != null) {
                        // Have to propagate error to user
                        throw e;
                     }
                     // If was error and no segment specific just delete entire thing
                     destroyDatabase = true;
                  }
               } else {
                  destroyDatabase = true;
               }
            } catch (Exception e) {
               throw new PersistenceException(e);
            }

            if (destroyDatabase) {
               try {
                  reinitAllDatabases();
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
      }

      protected void reinitAllDatabases() throws RocksDBException {
         db.close();
         expiredDb.close();
         if (System.getProperty("os.name").startsWith("Windows")) {
            // Force a GC to ensure that open file handles are released in Windows.
            System.gc();
         }
         Path dataLocation = getLocation();
         Util.recursiveFileRemove(dataLocation.toFile());
         db = open(getLocation(), dataDbOptions());

         Path expirationLocation = getExpirationLocation();
         Util.recursiveFileRemove(expirationLocation.toFile());
         expiredDb = openDatabase(expirationLocation, expiredDbOptions());
      }

      protected RocksIterator wrapIterator(RocksDB db, ReadOptions readOptions, int segment) {
         // Some Cache Store tests use clear and in case of the Rocks DB implementation
         // this clears out internal references and results in throwing exceptions
         // when getting an iterator. Unfortunately there is no nice way to check that...
         return db.newIterator(defaultColumnFamilyHandle, readOptions);
      }

      @Override
      Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue) {
         Predicate<? super K> combinedFilter = PersistenceUtil.combinePredicate(segments, keyPartitioner, filter);
         return publish(-1, combinedFilter);
      }

      @Override
      CompletionStage<Long> approximateSize(IntSet segments) {
         return size(segments);
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
   }

   private class SegmentedRocksDBHandler extends RocksDBHandler {
      private final AtomicReferenceArray<ColumnFamilyHandle> handles;

      private SegmentedRocksDBHandler(int segmentCount) {
         this.handles = new AtomicReferenceArray<>(segmentCount);
      }

      byte[] byteArrayFromInt(int val) {
         return new byte[] {
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
         List<ColumnFamilyDescriptor> descriptors = new ArrayList<>(segmentCount + 1);
         List<ColumnFamilyHandle> outHandles = new ArrayList<>(segmentCount + 1);
         // You have to open the default column family
         descriptors.add(new ColumnFamilyDescriptor(
               RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
         for (int i = 0; i < segmentCount; ++i) {
            descriptors.add(newDescriptor(byteArrayFromInt(i)));
         }
         RocksDB rocksDB = RocksDB.open(options, location.toString(), descriptors, outHandles);
         for (int i = 0; i < segmentCount; ++i) {
            handles.set(i, outHandles.get(i + 1));
         }
         return rocksDB;
      }

      @Override
      CompletionStage<Void> clear() {
         return blockingManager.runBlocking(() -> {
            for (int i = 0; i < handles.length(); ++i) {
               if (!clearForSegment(i)) {
                  recreateColumnFamily(i);
               }
            }
         }, "rocksdb-clear");
      }

      /**
       * Attempts to clear out the entries for a segment by using an iterator and deleting. If however an iterator
       * goes above the clear threshold it will immediately stop and return false. If it was able to remove all
       * the entries it will instead return true
       * @param segment the segment to clear out
       * @return whether it was able to clear all entries for the segment
       */
      private boolean clearForSegment(int segment) {
         int clearThreshold = configuration.clearThreshold();
         // If we always have to recreate don't even create iterator
         if (clearThreshold <= 0) {
            return false;
         }
         try (ReadOptions readOptions = new ReadOptions().setFillCache(false)) {
            RocksIterator optionalIterator = wrapIterator(db, readOptions, segment);
            if (optionalIterator != null) {
               ColumnFamilyHandle handle = handles.get(segment);
               try (RocksIterator it = optionalIterator) {
                  long count = 0;
                  for (it.seekToFirst(); it.isValid(); it.next()) {
                     byte[] keyBytes = it.key();
                     db.delete(handle, keyBytes);

                     if (++count > configuration.clearThreshold()) {
                        return false;
                     }
                  }
               } catch (RocksDBException e) {
                  throw new PersistenceException(e);
               }
               return true;
            } else {
               // If optional iterator was null that means either we don't own this segment or it was just
               // recrated - in either case we can consider that cleared
               return true;
            }
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
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
      }

      private void recreateColumnFamily(int segment) {
         ColumnFamilyHandle handle = handles.get(segment);
         if (handle != null) {
            try {
               db.dropColumnFamily(handle);
               handle = db.createColumnFamily(newDescriptor(byteArrayFromInt(segment)));
               handles.set(segment, handle);
            } catch (RocksDBException e) {
               throw new PersistenceException(e);
            }
         }
      }

      @Override
      Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue) {
         // Short circuit if only a single segment - assumed to be invoked from persistence thread
         if (segments != null && segments.size() == 1) {
            return publish(segments.iterator().nextInt(), filter);
         }
         IntSet segmentsToUse = segments == null ? IntSets.immutableRangeSet(handles.length()) : segments;
         return Flowable.fromIterable(segmentsToUse)
                 .concatMap(i -> publish(i, filter));
      }

      @Override
      CompletionStage<Long> approximateSize(IntSet segments) {
         return blockingManager.subscribeBlockingCollector(Flowable.fromIterable(segments), Collectors.summingLong(segment -> {
            ColumnFamilyHandle handle = getHandle(segment);
            try {
               return Long.parseLong(db.getProperty(handle, "rocksdb.estimate-num-keys"));
            } catch (RocksDBException e) {
               throw new PersistenceException(e);
            }
         }), "rocksdb-approximateSize");
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
            if (trace) {
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
            if (trace) {
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
   }

   private void putExpireDbData(ExpiryEntry entry) throws RocksDBException {
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
}
