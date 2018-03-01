package org.infinispan.persistence.rocksdb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration;
import org.infinispan.persistence.rocksdb.logging.Log;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import io.reactivex.Flowable;

@Store
@ConfiguredBy(RocksDBStoreConfiguration.class)
public class RocksDBStore<K,V> implements AdvancedLoadWriteStore<K,V> {
    private static final Log log = LogFactory.getLog(RocksDBStore.class, Log.class);

    private RocksDBStoreConfiguration configuration;
    private BlockingQueue<ExpiryEntry> expiryEntryQueue;
    private RocksDB db;
    private RocksDB expiredDb;
    private InitializationContext ctx;
    private Semaphore semaphore;
    private WriteOptions dataWriteOptions;
    private volatile boolean stopped = true;

    @Override
    public void init(InitializationContext ctx) {
        this.configuration = ctx.getConfiguration();
        this.ctx = ctx;
        this.semaphore = new Semaphore(Integer.MAX_VALUE, true);
    }

    @Override
    public void start() {
        expiryEntryQueue = new LinkedBlockingQueue<>(configuration.expiryQueueSize());

        try {
            db = openDatabase(getQualifiedLocation(), dataDbOptions());
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

    private Options dataDbOptions() {
        Options options = new Options().setCreateIfMissing(true);
        options.setCompressionType(CompressionType.getCompressionType(configuration.compressionType().toString()));

        return options;
    }

    private Options expiredDbOptions() {
        return new Options().setCreateIfMissing(true);
    }

    /**
     * Creates database if it doesn't exist.
     */
    protected RocksDB openDatabase(String location, Options options) throws IOException, RocksDBException {
        File dir = new File(location);
        dir.mkdirs();
        return RocksDB.open(options, location);
    }

    protected void destroyDatabase(String location) throws IOException {
        // Force a GC to ensure that open file handles are released in Windows
        System.gc();
        Util.recursiveFileRemove(new File(location));
    }

    protected RocksDB reinitDatabase(String location, Options options) throws IOException, RocksDBException {
        destroyDatabase(location);
        return openDatabase(location, options);
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
            db = reinitDatabase(getQualifiedLocation(), dataDbOptions());
            expiredDb = reinitDatabase(getQualifiedExpiredLocation(), expiredDbOptions());
        } finally {
            semaphore.release(Integer.MAX_VALUE);
        }
    }

    @Override
    public void stop() {
        try {
            semaphore.acquire(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            throw new PersistenceException("Cannot acquire semaphore", e);
        }
        try {
            db.close();
            expiredDb.close();
        } finally {
            stopped = true;
            semaphore.release(Integer.MAX_VALUE);
        }
    }

    @Override
    public void clear() {
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
            Optional<RocksIterator> optionalIterator = wrapIterator(this.db);
            if (optionalIterator.isPresent() && configuration.clearThreshold() <= 0) {
                try (RocksIterator it = optionalIterator.get()) {
                    for(it.seekToFirst(); it.isValid(); it.next()) {
                        db.delete(it.key());
                        count++;

                        if (count > configuration.clearThreshold()) {
                            destroyDatabase = true;
                            break;
                        }
                    }
                } catch (RocksDBException e) {
                    destroyDatabase = true;
                }
            } else {
                destroyDatabase = true;
            }
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

    private static Optional<RocksIterator> wrapIterator(RocksDB db) {
        // Some Cache Store tests use clear and in case of the Rocks DB implementation
        // this clears out internal references and results in throwing exceptions
        // when getting an iterator. Unfortunately there is no nice way to check that...
        return Optional.of(db.newIterator(new ReadOptions().setFillCache(false)));
    }

    @Override
    public int size() {
        return PersistenceUtil.count(this, null);
    }

    @Override
    public boolean contains(Object key) {
        try {
            return load(key) != null;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    private <P> Flowable<P> publish(Function<RocksIterator, Flowable<P>> function) {
        return Flowable.using(() -> {
            semaphore.acquire();
            if (stopped) {
                throw new PersistenceException("RocksDB is stopped");
            }
            return wrapIterator(this.db);
        }, opIterator -> {
            if (!opIterator.isPresent()) {
                return Flowable.empty();
            }
            RocksIterator it = opIterator.get();
            it.seekToFirst();
            return function.apply(it);
        }, opIterator -> {
            opIterator.ifPresent(RocksIterator::close);
            semaphore.release();
        });
    }

    @Override
    public Publisher<K> publishKeys(Predicate<? super K> filter) {
        return publish(it -> Flowable.fromIterable(() ->
            new AbstractIterator<K>() {
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
            }));
    }

    @Override
    public Publisher<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
        return publish(it -> Flowable.fromIterable(() -> {
            // Make sure this is taken when the iterator is created
            long now = ctx.getTimeService().wallClockTime();
            return new AbstractIterator<MarshalledEntry<K, V>>() {
                @Override
                protected MarshalledEntry<K, V> getNext() {
                    MarshalledEntry<K, V> entry = null;
                    try {
                        while (entry == null && it.isValid()) {
                            K key = (K) unmarshall(it.key());
                            if (filter == null || filter.test(key)) {
                                if (fetchValue || fetchMetadata) {
                                    MarshalledEntry<K, V> unmarshalledEntry = (MarshalledEntry<K, V>) unmarshall(
                                          it.value());
                                    InternalMetadata metadata = unmarshalledEntry.getMetadata();
                                    if (metadata == null || !metadata.isExpired(now)) {
                                        if (fetchMetadata && fetchValue) {
                                            entry = unmarshalledEntry;
                                        } else {
                                            // Sad that this has to make another entry!
                                            entry = ctx.getMarshalledEntryFactory().newMarshalledEntry(key,
                                                  fetchValue ? unmarshalledEntry.getValue() : null,
                                                  fetchMetadata ? unmarshalledEntry.getMetadata() : null);
                                        }
                                    }
                                } else {
                                    entry = ctx.getMarshalledEntryFactory().newMarshalledEntry(key, (Object) null, null);
                                }
                            }
                            it.next();
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        throw new CacheException(e);
                    }
                    return entry;
                }
            };
        }));
    }

    @Override
    public boolean delete(Object key) {
        try {
            byte[] keyBytes = marshall(key);
            semaphore.acquire();
            try {
                if (stopped) {
                    throw new PersistenceException("RocksDB is stopped");
                }
                if (db.get(keyBytes) == null) {
                    return false;
                }
                db.delete(keyBytes);
            } finally {
                semaphore.release();
            }
            return true;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void write(MarshalledEntry me) {
        try {
            byte[] marshelledKey = marshall(me.getKey());
            byte[] marshalledEntry = marshall(me);
            semaphore.acquire();
            try {
                if (stopped) {
                    throw new PersistenceException("RocksDB is stopped");
                }
                db.put(marshelledKey, marshalledEntry);
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

    @Override
    public MarshalledEntry load(Object key) {
        try {
            byte[] marshalledEntry;
            semaphore.acquire();
            try {
                if (stopped) {
                    throw new PersistenceException("RocksDB is stopped");
                }
                marshalledEntry = db.get(marshall(key));
            } finally {
                semaphore.release();
            }
            MarshalledEntry me = (MarshalledEntry) unmarshall(marshalledEntry);
            if (me == null) return null;

            InternalMetadata meta = me.getMetadata();
            if (meta != null && meta.isExpired(ctx.getTimeService().wallClockTime())) {
                return null;
            }
            return me;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void writeBatch(Iterable<MarshalledEntry<? extends K, ? extends V>> marshalledEntries) {
        try {
            int batchSize = 0;
            WriteBatch batch = new WriteBatch();
            for (MarshalledEntry entry : marshalledEntries) {
                batch.put(marshall(entry.getKey()), marshall(entry));
                batchSize++;

                if (batchSize == configuration.maxBatchSize()) {
                    batchSize = 0;
                    writeBatch(batch);
                    batch = new WriteBatch();
                }
            }

            if (batchSize != 0)
                writeBatch(batch);

            // Add metadata only after batch has been written
            for (MarshalledEntry entry : marshalledEntries) {
                InternalMetadata meta = entry.getMetadata();
                if (meta != null && meta.expiryTime() > -1)
                    addNewExpiry(entry);
            }
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

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
            Optional<RocksIterator> optionalIterator = wrapIterator(expiredDb);
            if (optionalIterator.isPresent()) {
                try (RocksIterator it = optionalIterator.get()) {
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
                        byte[] keyBytes = marshall(key);

                        byte[] b = db.get(keyBytes);
                        if (b == null)
                            continue;
                        MarshalledEntry me = (MarshalledEntry) ctx.getMarshaller().objectFromByteBuffer(b);
                        // TODO race condition: the entry could be updated between the get and delete!
                        if (me.getMetadata() != null && me.getMetadata().isExpired(now)) {
                            // somewhat inefficient to FIND then REMOVE...
                            db.delete(keyBytes);
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

    private byte[] marshall(Object entry) throws IOException, InterruptedException {
        return ctx.getMarshaller().objectToByteBuffer(entry);
    }

    private Object unmarshall(byte[] bytes) throws IOException, ClassNotFoundException {
        if (bytes == null)
            return null;

        return ctx.getMarshaller().objectFromByteBuffer(bytes);
    }

    private void addNewExpiry(MarshalledEntry entry) throws IOException {
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

    private static final class Entry {
        final byte[] key;
        final byte[] value;

        Entry(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

}
