package org.infinispan.persistence.rocksdb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.internal.PersistenceUtil;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration;
import org.infinispan.persistence.rocksdb.logging.Log;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.internal.functions.Functions;
import io.reactivex.schedulers.Schedulers;

@Store
@ConfiguredBy(RocksDBStoreConfiguration.class)
public class RocksDBStore<K,V> implements SegmentedAdvancedLoadWriteStore<K,V> {
    private static final Log log = LogFactory.getLog(RocksDBStore.class, Log.class);
    static final String databasePropertyNameWithSuffix = "database.";
    static final String columnFamilyPropertyNameWithSuffix = "data.";

    private RocksDBStoreConfiguration configuration;
    private BlockingQueue<ExpiryEntry> expiryEntryQueue;
    private RocksDB db;
    private RocksDB expiredDb;
    private InitializationContext ctx;
    private Scheduler scheduler;
    private TimeService timeService;
    private Semaphore semaphore;
    private WriteOptions dataWriteOptions;
    private RocksDBHandler handler;
    private Properties databaseProperties;
    private Properties columnFamilyProperties;
    private volatile boolean stopped = true;

    @Override
    public void init(InitializationContext ctx) {
        this.configuration = ctx.getConfiguration();
        this.ctx = ctx;
        this.scheduler = Schedulers.from(ctx.getExecutor());
        this.timeService = ctx.getTimeService();
        this.semaphore = new Semaphore(Integer.MAX_VALUE, true);
    }

    @Override
    public void start() {
        expiryEntryQueue = new LinkedBlockingQueue<>(configuration.expiryQueueSize());

        AdvancedCache cache = ctx.getCache().getAdvancedCache();
        ComponentRegistry registry = cache.getComponentRegistry();
        KeyPartitioner keyPartitioner = registry.getComponent(KeyPartitioner.class);
        if (configuration.segmented()) {
            handler = new SegmentedRocksDBHandler(cache.getCacheConfiguration().clustering().hash().numSegments(),
                  keyPartitioner);
        } else {
            handler = new NonSegmentedRocksDBHandler(keyPartitioner);
        }

        // Has to be done before we open the database, so we can pass the properties
        Properties allProperties = configuration.properties();
        for (Map.Entry<Object, Object> entry : allProperties.entrySet()) {
            String key = entry.getKey().toString();
            if (key.startsWith(databasePropertyNameWithSuffix)) {
                if (databaseProperties == null) {
                    databaseProperties = new Properties();
                }
                databaseProperties.setProperty(key.substring(databasePropertyNameWithSuffix.length()), entry.getValue().toString());
            } else if (key.startsWith(columnFamilyPropertyNameWithSuffix)) {
                if (columnFamilyProperties == null) {
                    columnFamilyProperties = new Properties();
                }
                columnFamilyProperties.setProperty(key.substring(columnFamilyPropertyNameWithSuffix.length()), entry.getValue().toString());
            }
        }

        try {
            db = handler.open(getQualifiedLocation(), dataDbOptions());
            expiredDb = openDatabase(getQualifiedExpiredLocation(), expiredDbOptions());
            stopped = false;
        } catch (Exception e) {
            throw new CacheConfigurationException("Unable to open database", e);
        }
    }

    private String sanitizedCacheName() {
        return ctx.getCache().getName().replaceAll("[^a-zA-Z0-9-_\\.]", "_");
    }

    private String getQualifiedLocation() {
        return configuration.location() + sanitizedCacheName();
    }

    private String getQualifiedExpiredLocation() {
        return configuration.expiredLocation() + sanitizedCacheName();
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

    private Options expiredDbOptions() {
        return new Options()
              .setCreateIfMissing(true)
              // Make sure keys are sorted by bytes - we use this sorting to remove entries that have expired most recently
              .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
    }

    /**
     * Creates database if it doesn't exist.
     */
    protected RocksDB openDatabase(String location, Options options) throws IOException, RocksDBException {
        File dir = new File(location);
        dir.mkdirs();
        return RocksDB.open(options, location);
    }

    @Override
    public void stop() {
        try {
            semaphore.acquire(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            throw new PersistenceException("Cannot acquire semaphore", e);
        }
        try {
            handler.close();
            expiredDb.close();
        } finally {
            stopped = true;
            semaphore.release(Integer.MAX_VALUE);
        }
    }

    @Override
    public void destroy() {
        stop();

        Util.recursiveFileRemove(new File(getQualifiedLocation()));

        Util.recursiveFileRemove(new File(getQualifiedExpiredLocation()));
    }

    @Override
    public boolean isAvailable() {
        return new File(getQualifiedLocation()).exists() && new File(getQualifiedExpiredLocation()).exists();
    }

    @Override
    public void clear() {
        handler.clear(null);
    }

    @Override
    public void clear(IntSet segments) {
        handler.clear(segments);
    }

    @Override
    public int size() {
        return handler.size(null);
    }

    @Override
    public int size(IntSet segments) {
        return handler.size(segments);
    }

    @Override
    public boolean contains(Object key) {
        return handler.contains(-1, key);
    }

    @Override
    public boolean contains(int segment, Object key) {
        return handler.contains(segment, key);
    }

    @Override
    public Publisher<K> publishKeys(Predicate<? super K> filter) {
        return handler.publishKeys(null, filter);
    }

    @Override
    public Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
        return handler.publishKeys(segments, filter);
    }

    @Override
    public Publisher<MarshallableEntry<K, V>> entryPublisher(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
        return handler.publishEntries(null, filter, fetchValue, fetchMetadata);
    }

    @Override
    public Publisher<MarshallableEntry<K, V>> entryPublisher(IntSet segments, Predicate<? super K> filter,
                                                             boolean fetchValue, boolean fetchMetadata) {
        return handler.publishEntries(segments, filter, fetchValue, fetchMetadata);
    }

    @Override
    public boolean delete(Object key) {
        return handler.delete(-1, key);
    }

    @Override
    public boolean delete(int segment, Object key) {
        return handler.delete(segment, key);
    }

    @Override
    public void write(MarshallableEntry entry) {
        handler.write(-1, entry);
    }

    @Override
    public void write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
        handler.write(segment, entry);
    }

    @Override
    public MarshallableEntry loadEntry(Object key) {
        return handler.load(-1, key);
    }

    @Override
    public MarshallableEntry<K, V> get(int segment, Object key) {
        return handler.load(segment, key);
    }

    @Override
    public CompletionStage<Void> bulkUpdate(Publisher<MarshallableEntry<? extends K, ? extends V>> publisher) {
        return handler.writeBatch(publisher);
    }

    @Override
    public void deleteBatch(Iterable<Object> keys) {
        handler.deleteBatch(keys);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void purge(Executor executor, PurgeListener purgeListener) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new PersistenceException("Cannot acquire semaphore: CacheStore is likely stopped.", e);
        }
        try {
            if (stopped) {
                throw new PersistenceException("RocksDB is stopped");
            }
            // Drain queue and update expiry tree
            List<ExpiryEntry> entries = new ArrayList<>();
            expiryEntryQueue.drainTo(entries);
            for (ExpiryEntry entry : entries) {
                final byte[] expiryBytes = marshall(entry.expiry);
                final byte[] keyBytes = marshall(entry.key);
                final byte[] existingBytes = expiredDb.get(expiryBytes);

                if (existingBytes != null) {
                    // in the case of collision make the key a List ...
                    final Object existing = unmarshall(existingBytes);
                    if (existing instanceof List) {
                        ((List<Object>) existing).add(entry.key);
                        expiredDb.put(expiryBytes, marshall(existing));
                    } else {
                        List<Object> al = new ArrayList<>(2);
                        al.add(existing);
                        al.add(entry.key);
                        expiredDb.put(expiryBytes, marshall(al));
                    }
                } else {
                    expiredDb.put(expiryBytes, keyBytes);
                }
            }

            List<Long> times = new ArrayList<>();
            List<Object> keys = new ArrayList<>();

            long now = ctx.getTimeService().wallClockTime();
            RocksIterator iterator = expiredDb.newIterator(new ReadOptions().setFillCache(false));
            if (iterator != null) {
                try (RocksIterator it = iterator) {
                    for (it.seekToFirst(); it.isValid(); it.next()) {
                        Long time = (Long) unmarshall(it.key());
                        if (time > now)
                            break;
                        times.add(time);
                        Object key = unmarshall(it.value());
                        if (key instanceof List)
                            keys.addAll((List<?>) key);
                        else
                            keys.add(key);
                    }

                    for (Long time : times) {
                        expiredDb.delete(marshall(time));
                    }

                    if (!keys.isEmpty())
                        log.debugf("purge (up to) %d entries", keys.size());
                    int count = 0;
                    for (Object key : keys) {
                        int segment = handler.calculateSegment(key);

                        ColumnFamilyHandle handle = handler.getHandle(segment);
                        byte[] keyBytes = marshall(key);
                        byte[] valueBytes = db.get(handle, keyBytes);
                        if (valueBytes == null)
                            continue;
                        MarshallableEntry me = getMarshallableEntryFromValue(key, valueBytes);
                        // TODO race condition: the entry could be updated between the get and delete!
                        if (me.getMetadata() != null && me.getMetadata().isExpired(now)) {
                            // somewhat inefficient to FIND then REMOVE...
                            db.delete(handle, keyBytes);
                            purgeListener.entryPurged(key);
                            count++;
                        }
                    }
                    if (count != 0)
                        log.debugf("purged %d entries", count);
                } catch (Exception e) {
                    throw new PersistenceException(e);
                }
            }
        } catch (PersistenceException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistenceException(e);
        } finally {
            semaphore.release();
        }
    }

    @Override
    public void addSegments(IntSet segments) {
        handler.addSegments(segments);
    }

    @Override
    public void removeSegments(IntSet segments) {
        handler.removeSegments(segments);
    }

    private byte[] marshall(Object entry) throws IOException, InterruptedException {
        return ctx.getMarshaller().objectToByteBuffer(entry);
    }

    private Object unmarshall(byte[] bytes) throws IOException, ClassNotFoundException {
        if (bytes == null)
            return null;

        return ctx.getMarshaller().objectFromByteBuffer(bytes);
    }

    private byte[] getMarshalledKeyValuePair(MarshallableEntry me) throws IOException, InterruptedException {
        byte[] value = me.getValueBytes() != null ? me.getValueBytes().getBuf() : null;
        byte[] meta = me.getMetadataBytes() != null ? me.getMetadataBytes().getBuf() : null;
        KeyValuePair<byte[], byte[]> kvp = new KeyValuePair<>(value, meta);
        return marshall(kvp);
    }

    private MarshallableEntry getMarshallableEntryFromValue(Object key, byte[] valueBytes) throws IOException, ClassNotFoundException {
        KeyValuePair<byte[], byte[]> kvp = (KeyValuePair<byte[], byte[]>) unmarshall(valueBytes);
        if (kvp == null) return null;

        ByteBuffer value = kvp.getKey() != null ? new ByteBufferImpl(kvp.getKey()) : null;
        ByteBuffer metadata = kvp.getValue() != null ? new ByteBufferImpl(kvp.getValue()) : null;
        return ctx.getMarshallableEntryFactory().create(key, value, metadata);
    }

    private void addNewExpiry(MarshallableEntry entry) throws IOException {
        long expiry = entry.getMetadata().expiryTime();
        long maxIdle = entry.getMetadata().maxIdle();
        if (maxIdle > 0) {
            // Coding getExpiryTime() for transient entries has the risk of being a moving target
            // which could lead to unexpected results, hence, InternalCacheEntry calls are required
            expiry = maxIdle + ctx.getTimeService().wallClockTime();
        }
        Long at = expiry;
        Object key = entry.getKey();

        try {
            expiryEntryQueue.put(new ExpiryEntry(at, key));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interruption status
        }
    }

    private static final class ExpiryEntry {
        private final Long expiry;
        private final Object key;

        private ExpiryEntry(long expiry, Object key) {
            this.expiry = expiry;
            this.key = key;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ExpiryEntry other = (ExpiryEntry) obj;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            return true;
        }

    }

    private class RocksKeyIterator extends AbstractIterator<K> {
        private final RocksIterator it;
        private final Predicate<? super K> filter;

        public RocksKeyIterator(RocksIterator it, Predicate<? super K> filter) {
            this.it = it;
            this.filter = filter;
        }

        @Override
        protected K getNext() {
            K key = null;
            try {
                while (key == null && it.isValid()) {
                    K testKey = (K) unmarshall(it.key());
                    if (filter == null || filter.test(testKey)) {
                        key = testKey;
                    }
                    it.next();
                }
            } catch (IOException | ClassNotFoundException e) {
                throw new CacheException(e);
            }
            return key;
        }
    }

    private class RocksEntryIterator extends AbstractIterator<MarshallableEntry<K, V>> {
        private final RocksIterator it;
        private final Predicate<? super K> filter;
        private final boolean fetchValue;
        private final boolean fetchMetadata;
        private final long now;

        public RocksEntryIterator(RocksIterator it, Predicate<? super K> filter, boolean fetchValue,
              boolean fetchMetadata, long now) {
            this.it = it;
            this.filter = filter;
            this.fetchValue = fetchValue;
            this.fetchMetadata = fetchMetadata;
            this.now = now;
        }

        @Override
        protected MarshallableEntry<K, V> getNext() {
            MarshallableEntry<K, V> entry = null;
            try {
                while (entry == null && it.isValid()) {
                    K key = (K) unmarshall(it.key());
                    if (filter == null || filter.test(key)) {
                        if (fetchValue || fetchMetadata) {
                            MarshallableEntry<K, V> unmarshalledEntry = getMarshallableEntryFromValue(key, it.value());
                            InternalMetadata metadata = unmarshalledEntry.getMetadata();
                            if (metadata == null || !metadata.isExpired(now)) {
                                if (fetchMetadata && fetchValue) {
                                    entry = unmarshalledEntry;
                                } else {
                                    // Sad that this has to make another entry!
                                    entry = ctx.getMarshallableEntryFactory().create(key,
                                          fetchValue ? unmarshalledEntry.getValue() : null,
                                          fetchMetadata ? unmarshalledEntry.getMetadata() : null);
                                }
                            }
                        } else {
                            entry = ctx.getMarshallableEntryFactory().create(key, (Object) null, null);
                        }
                    }
                    it.next();
                }
            } catch (IOException | ClassNotFoundException e) {
                throw new CacheException(e);
            }
            return entry;
        }
    }

    private abstract class RocksDBHandler {

        abstract RocksDB open(String location, DBOptions options) throws RocksDBException;

        abstract void close();

        abstract ColumnFamilyHandle getHandle(int segment);

        final ColumnFamilyHandle getHandle(int segment, Object key) {
            if (segment < 0) {
                segment = calculateSegment(key);
            }
            return getHandle(segment);
        }

        abstract int calculateSegment(Object key);

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
            return new ColumnFamilyDescriptor(name,
                  columnFamilyOptions.setCompressionType(CompressionType.getCompressionType(configuration.compressionType().toString())));
        }

        boolean contains(int segment, Object key) {
            // This might be able to use RocksDB#keyMayExist - but API is a bit flaky
            return load(segment, key) != null;
        }

        MarshallableEntry<K, V> load(int segment, Object key) {
            ColumnFamilyHandle handle = getHandle(segment, key);
            if (handle == null) {
                log.trace("Ignoring load as handle is not currently configured");
                return null;
            }
            try {
                byte[] kvpBytes;
                semaphore.acquire();
                try {
                    if (stopped) {
                        throw new PersistenceException("RocksDB is stopped");
                    }

                    kvpBytes = db.get(handle, marshall(key));
                } finally {
                    semaphore.release();
                }
                MarshallableEntry me = getMarshallableEntryFromValue(key, kvpBytes);
                if (me == null) return null;

                InternalMetadata meta = me.getMetadata();
                if (meta != null && meta.isExpired(timeService.wallClockTime())) {
                    return null;
                }
                return me;
            } catch (Exception e) {
                throw new PersistenceException(e);
            }
        }

        void write(int segment, MarshallableEntry<? extends K, ? extends V> me) {
            Object key = me.getKey();
            ColumnFamilyHandle handle = getHandle(segment, key);
            if (handle == null) {
                log.trace("Ignoring write as handle is not currently configured");
                return;
            }
            try {
                byte[] marshalledKey = marshall(key);
                byte[] marshalledValue = getMarshalledKeyValuePair(me);
                semaphore.acquire();
                try {
                    if (stopped) {
                        throw new PersistenceException("RocksDB is stopped");
                    }

                    db.put(handle, marshalledKey, marshalledValue);
                } finally {
                    semaphore.release();
                }
                InternalMetadata meta = me.getMetadata();
                if (meta != null && meta.expiryTime() > -1) {
                    addNewExpiry(me);
                }
            } catch (Exception e) {
                throw new PersistenceException(e);
            }
        }

        boolean delete(int segment, Object key) {
            try {
                byte[] keyBytes = marshall(key);
                semaphore.acquire();
                try {
                    if (stopped) {
                        throw new PersistenceException("RocksDB is stopped");
                    }
                    if (db.get(getHandle(segment, key), keyBytes) == null) {
                        return false;
                    }
                    db.delete(getHandle(segment, key), keyBytes);
                } finally {
                    semaphore.release();
                }
                return true;
            } catch (Exception e) {
                throw new PersistenceException(e);
            }
        }

        CompletableFuture<Void> writeBatch(Publisher<MarshallableEntry<? extends K, ? extends V>> publisher) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            Flowable.fromPublisher(publisher)
                  .buffer(configuration.maxBatchSize())
                  .doOnNext(entries -> {
                      WriteBatch batch = new WriteBatch();
                      for (MarshallableEntry<? extends K, ? extends V> entry : entries) {
                          Object key = entry.getKey();
                          batch.put(getHandle(calculateSegment(key)), marshall(key), getMarshalledKeyValuePair(entry));
                      }
                      writeBatch(batch);

                      // Add metadata only after batch has been written
                      for (MarshallableEntry entry : entries) {
                          InternalMetadata meta = entry.getMetadata();
                          if (meta != null && meta.expiryTime() > -1)
                              addNewExpiry(entry);
                      }
                  })
                  .doOnError(e -> {
                      throw new PersistenceException(e);
                  })
                  .subscribe(Functions.emptyConsumer(), future::completeExceptionally, () -> future.complete(null));
            return future;
        }

        void deleteBatch(Iterable<Object> keys) {
            try {
                int batchSize = 0;
                WriteBatch batch = new WriteBatch();
                for (Object key : keys) {
                    batch.remove(getHandle(calculateSegment(key)), marshall(key));
                    batchSize++;

                    if (batchSize == configuration.maxBatchSize()) {
                        batchSize = 0;
                        writeBatch(batch);
                        batch = new WriteBatch();
                    }
                }

                if (batchSize != 0)
                    writeBatch(batch);
            } catch (Exception e) {
                throw new PersistenceException(e);
            }
        }

        abstract void clear(IntSet segments);

        abstract Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter);

        abstract Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter,
                                                                   boolean fetchValue, boolean fetchMetadata);

        int size(IntSet segments) {
            long count = Flowable.fromPublisher(publishKeys(segments, null))
                  .count().blockingGet();
            if (count > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            return (int) count;
        }

        <P> Flowable<P> publish(int segment, Function<RocksIterator, Flowable<P>> function) {
            return Flowable.using(() -> {
                semaphore.acquire();
                if (stopped) {
                    throw new PersistenceException("RocksDB is stopped");
                }
                return wrapIterator(db, segment);
            }, iterator -> {
                if (iterator == null) {
                    return Flowable.empty();
                }
                iterator.seekToFirst();
                return function.apply(iterator);
            }, iterator -> {
                if (iterator != null) {
                    iterator.close();
                }
                semaphore.release();
            });
        }

        abstract RocksIterator wrapIterator(RocksDB db, int segment);

        private void writeBatch(WriteBatch batch) throws InterruptedException, RocksDBException {
            semaphore.acquire();
            try {
                if (stopped)
                    throw new PersistenceException("RocksDB is stopped");

                db.write(dataWriteOptions(), batch);
            } finally {
                semaphore.release();
            }
        }

        abstract void addSegments(IntSet segments);

        abstract void removeSegments(IntSet segments);
    }

    private final class NonSegmentedRocksDBHandler extends RocksDBHandler {
        private final KeyPartitioner keyPartitioner;
        private ColumnFamilyHandle defaultColumnFamilyHandle;

        public NonSegmentedRocksDBHandler(KeyPartitioner keyPartitioner) {
            this.keyPartitioner = keyPartitioner;
        }

        @Override
        ColumnFamilyHandle getHandle(int segment) {
            return defaultColumnFamilyHandle;
        }

        @Override
        int calculateSegment(Object key) {
            // Segment not used
            return 0;
        }

        @Override
        RocksDB open(String location, DBOptions options) throws RocksDBException {
            File dir = new File(location);
            dir.mkdirs();
            List<ColumnFamilyHandle> handles = new ArrayList<>(1);
            RocksDB rocksDB = RocksDB.open(options, location,
                  Collections.singletonList(newDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY)),
                  handles);
            defaultColumnFamilyHandle = handles.get(0);
            return rocksDB;
        }

        @Override
        void clear(IntSet segments) {
            long count = 0;
            boolean destroyDatabase = false;
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                throw new PersistenceException("Cannot acquire semaphore", e);
            }
            try {
                if (stopped) {
                    throw new PersistenceException("RocksDB is stopped");
                }
                RocksIterator optionalIterator = wrapIterator(db, -1);
                if (optionalIterator != null && (configuration.clearThreshold() > 0 || segments == null)) {
                    try (RocksIterator it = optionalIterator) {
                        for (it.seekToFirst(); it.isValid(); it.next()) {
                            byte[] keyBytes = it.key();
                            if (segments != null) {
                                Object key = unmarshall(keyBytes);
                                if (segments.contains(keyPartitioner.getSegment(key))) {
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
            } finally {
                semaphore.release();
            }

            if (destroyDatabase) {
                try {
                    reinitAllDatabases();
                } catch (Exception e) {
                    throw new PersistenceException(e);
                }
            }
        }

        @Override
        void close() {
            defaultColumnFamilyHandle.close();

            db.close();
        }

        protected void reinitAllDatabases() throws IOException, RocksDBException {
            try {
                semaphore.acquire(Integer.MAX_VALUE);
            } catch (InterruptedException e) {
                throw new PersistenceException("Cannot acquire semaphore", e);
            }
            try {
                if (stopped) {
                    throw new PersistenceException("RocksDB is stopped");
                }
                db.close();
                expiredDb.close();
                // Force a GC to ensure that open file handles are released in Windows
                System.gc();
                String dataLocation = getQualifiedLocation();
                Util.recursiveFileRemove(new File(dataLocation));
                db = open(getQualifiedLocation(), dataDbOptions());

                String expirationLocation = getQualifiedExpiredLocation();
                Util.recursiveFileRemove(new File(expirationLocation));
                expiredDb = openDatabase(expirationLocation, expiredDbOptions());
            } finally {
                semaphore.release(Integer.MAX_VALUE);
            }
        }

        protected RocksIterator wrapIterator(RocksDB db, int segment) {
            // Some Cache Store tests use clear and in case of the Rocks DB implementation
            // this clears out internal references and results in throwing exceptions
            // when getting an iterator. Unfortunately there is no nice way to check that...
            return db.newIterator(defaultColumnFamilyHandle, new ReadOptions().setFillCache(false));
        }

        @Override
        Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
            Predicate<? super K> combinedFilter = PersistenceUtil.combinePredicate(segments, keyPartitioner, filter);
            return publish(-1, it -> Flowable.fromIterable(() -> new RocksKeyIterator(it, combinedFilter)));
        }

        @Override
        Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue,
                                                          boolean fetchMetadata) {
            Predicate<? super K> combinedFilter = PersistenceUtil.combinePredicate(segments, keyPartitioner, filter);
            return publish(-1, it -> Flowable.fromIterable(() -> {
                // Make sure this is taken when the iterator is created
                long now = timeService.wallClockTime();
                return new RocksEntryIterator(it, combinedFilter, fetchValue, fetchMetadata, now);
            }));
        }

        @Override
        void addSegments(IntSet segments) {
            // Do nothing
        }

        @Override
        void removeSegments(IntSet segments) {
            clear(segments);
        }
    }

    private class SegmentedRocksDBHandler extends RocksDBHandler {
        private final KeyPartitioner keyPartitioner;
        private final AtomicReferenceArray<ColumnFamilyHandle> handles;

        private SegmentedRocksDBHandler(int segmentCount, KeyPartitioner keyPartitioner) {
            this.keyPartitioner = keyPartitioner;
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
        int calculateSegment(Object key) {
            return keyPartitioner.getSegment(key);
        }

        @Override
        RocksDB open(String location, DBOptions options) throws RocksDBException {
            File dir = new File(location);
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
            RocksDB rocksDB = RocksDB.open(options, location, descriptors, outHandles);
            for (int i = 0; i < segmentCount; ++i) {
                handles.set(i, outHandles.get(i + 1));
            }
            return rocksDB;
        }

        @Override
        void clear(IntSet segments) {
            if (segments != null) {
                for (PrimitiveIterator.OfInt segmentIterator = segments.iterator(); segmentIterator.hasNext(); ) {
                    int segment = segmentIterator.nextInt();
                    if (!clearForSegment(segment)) {
                        recreateColumnFamily(segment);
                    }
                }
            } else {
                for (int i = 0; i < handles.length(); ++i) {
                    if (!clearForSegment(i)) {
                        recreateColumnFamily(i);
                    }
                }
            }
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
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                throw new PersistenceException("Cannot acquire semaphore", e);
            }
            try {
                if (stopped) {
                    throw new PersistenceException("RocksDB is stopped");
                }
                RocksIterator optionalIterator = wrapIterator(db, segment);
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
            } finally {
                semaphore.release();
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
        Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
            return PersistenceUtil.parallelizePublisher(segments == null ? IntSets.immutableRangeSet(handles.length()) : segments,
                  scheduler, i -> publish(i, it -> Flowable.fromIterable(() -> new RocksKeyIterator(it, filter))));
        }

        @Override
        Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
            return PersistenceUtil.parallelizePublisher(segments == null ? IntSets.immutableRangeSet(handles.length()) : segments,
                  scheduler, i -> publish(i, it -> Flowable.fromIterable(() -> {
                      long now = timeService.wallClockTime();
                      return new RocksEntryIterator(it, filter, fetchValue, fetchMetadata, now);
                  })));
        }

        @Override
        RocksIterator wrapIterator(RocksDB db, int segment) {
            ColumnFamilyHandle handle = handles.get(segment);
            if (handle != null) {
                return db.newIterator(handle, new ReadOptions().setFillCache(false));
            }
            return null;
        }

        @Override
        void addSegments(IntSet segments) {
            for (PrimitiveIterator.OfInt segmentIterator = segments.iterator(); segmentIterator.hasNext(); ) {
                int segment = segmentIterator.nextInt();
                ColumnFamilyHandle handle = handles.get(segment);
                if (handle == null) {
                    log.tracef("Creating column family for segment %d", segment);
                    byte[] cfName = byteArrayFromInt(segment);
                    try {
                        handle = db.createColumnFamily(newDescriptor(cfName));
                        handles.set(segment, handle);
                    } catch (RocksDBException e) {
                        throw new PersistenceException(e);
                    }
                }
            }
        }

        @Override
        void removeSegments(IntSet segments) {
            for (PrimitiveIterator.OfInt segmentIterator = segments.iterator(); segmentIterator.hasNext(); ) {
                int segment = segmentIterator.nextInt();
                ColumnFamilyHandle handle = handles.getAndSet(segment, null);
                if (handle != null) {
                    log.tracef("Dropping column family for segment %d", segment);
                    try {
                        db.dropColumnFamily(handle);
                    } catch (RocksDBException e) {
                        throw new PersistenceException(e);
                    }
                    handle.close();
                }
            }
        }
    }
}
