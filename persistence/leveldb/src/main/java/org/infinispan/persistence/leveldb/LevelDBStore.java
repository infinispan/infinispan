package org.infinispan.persistence.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.util.Util;
import org.infinispan.executors.ExecutorAllCompletionService;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.logging.Log;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;

@ConfiguredBy(LevelDBStoreConfiguration.class)
public class LevelDBStore implements AdvancedLoadWriteStore {
   private static final Log log = LogFactory.getLog(LevelDBStore.class, Log.class);

   private static final String JNI_DB_FACTORY_CLASS_NAME = "org.fusesource.leveldbjni.JniDBFactory";
   private static final String JAVA_DB_FACTORY_CLASS_NAME = "org.iq80.leveldb.impl.Iq80DBFactory";
   private static final String[] DB_FACTORY_CLASS_NAMES = new String[] { JNI_DB_FACTORY_CLASS_NAME, JAVA_DB_FACTORY_CLASS_NAME };

   private LevelDBStoreConfiguration configuration;
   private BlockingQueue<ExpiryEntry> expiryEntryQueue;
   private DBFactory dbFactory;
   private DB db;
   private DB expiredDb;
   private InitializationContext ctx;
   private Semaphore semaphore;
   private volatile boolean stopped = true;

   @Override
   public void init(InitializationContext ctx) {
      this.configuration = ctx.getConfiguration();
      this.dbFactory = newDbFactory();
      this.ctx = ctx;
      this.semaphore = new Semaphore(Integer.MAX_VALUE, true);

      if (this.dbFactory == null) {
         throw log.cannotLoadlevelDBFactories(Arrays.toString(DB_FACTORY_CLASS_NAMES));
      }
      String dbFactoryClassName = this.dbFactory.getClass().getName();
      if (dbFactoryClassName.equals("org.iq80.leveldb.impl.Iq80DBFactory")) {
         log.infoUsingJavaDbFactory(dbFactoryClassName);
      } else {
         log.infoUsingJNIDbFactory(dbFactoryClassName);
      }
   }

   protected DBFactory newDbFactory() {
      switch (configuration.implementationType()) {
         case JNI: {
            return Util.getInstance(JNI_DB_FACTORY_CLASS_NAME, LevelDBStore.class.getClassLoader());
         }
         case JAVA: {
            return Util.getInstance(JAVA_DB_FACTORY_CLASS_NAME, LevelDBStore.class.getClassLoader());
         }
         default: {
            for (String className : DB_FACTORY_CLASS_NAMES) {
               try {
                  return Util.getInstance(className, LevelDBStore.class.getClassLoader());
               } catch (Throwable e) {
                  if (log.isDebugEnabled())
                     log.debugUnableToInstantiateDbFactory(className, e);
               }
            }
         }
      }
      return null;
   }

   @Override
   public void start()  {
      expiryEntryQueue = new LinkedBlockingQueue<ExpiryEntry>(configuration.expiryQueueSize());

      try {
         db = openDatabase(getQualifiedLocation(), dataDbOptions());
         expiredDb = openDatabase(getQualifiedExpiredLocation(), expiredDbOptions());
         stopped = false;
      } catch (IOException e) {
         throw new CacheConfigurationException("Unable to open database", e);
      }
   }

   private String sanitizedCacheName() {
      String cacheFileName = ctx.getCache().getName().replaceAll("[^a-zA-Z0-9-_\\.]", "_");
      return cacheFileName;
   }

   private String getQualifiedLocation() {
      return configuration.location() + sanitizedCacheName();
   }

   private String getQualifiedExpiredLocation() {
      return configuration.expiredLocation() + sanitizedCacheName();
   }

   private Options dataDbOptions() {
      Options options = new Options().createIfMissing(true);

      options.compressionType(CompressionType.valueOf(configuration.compressionType().name()));

      if (configuration.blockSize() != null) {
         options.blockSize(configuration.blockSize());
      }

      if (configuration.cacheSize() != null) {
         options.cacheSize(configuration.cacheSize());
      }

      return options;
   }

   private Options expiredDbOptions() {
      return new Options().createIfMissing(true);
   }

   /**
    * Creates database if it doesn't exist.
    */
   protected DB openDatabase(String location, Options options) throws IOException {
      File dir = new File(location);

      // LevelDB JNI Option createIfMissing doesn't seem to work properly
      dir.mkdirs();
      return dbFactory.open(dir, options);
   }

   protected void destroyDatabase(String location) throws IOException {
      File dir = new File(location);
      dbFactory.destroy(dir, new Options());
   }

   protected DB reinitDatabase(String location, Options options) throws IOException {
      destroyDatabase(location);
      return openDatabase(location, options);
   }

   protected void reinitAllDatabases() throws IOException {
      try {
         semaphore.acquire(Integer.MAX_VALUE);
      } catch (InterruptedException e) {
         throw new PersistenceException("Cannot acquire semaphore", e);
      }
      try {
         if (stopped) {
            throw new PersistenceException("LevelDB is stopped");
         }
         try {
            db.close();
         } catch (IOException e) {
            log.warnUnableToCloseDb(e);
         }
         try {
            expiredDb.close();
         } catch (IOException e) {
            log.warnUnableToCloseExpiredDb(e);
         }
         db = reinitDatabase(getQualifiedLocation(), dataDbOptions());
         expiredDb = reinitDatabase(getQualifiedExpiredLocation(), expiredDbOptions());
      } finally {
         semaphore.release(Integer.MAX_VALUE);
      }
   }

   @Override
   public void stop()  {
      try {
         semaphore.acquire(Integer.MAX_VALUE);
      } catch (InterruptedException e) {
         throw new PersistenceException("Cannot acquire semaphore", e);
      }
      try {
         try {
            db.close();
         } catch (IOException e) {
            log.warnUnableToCloseDb(e);
         }

         try {
            expiredDb.close();
         } catch (IOException e) {
            log.warnUnableToCloseExpiredDb(e);
         }
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
            throw new PersistenceException("LevelDB is stopped");
         }
         DBIterator it = db.iterator(new ReadOptions().fillCache(false));
         if (configuration.clearThreshold() <= 0) {
            try {
               for (it.seekToFirst(); it.hasNext(); ) {
                  Map.Entry<byte[], byte[]> entry = it.next();
                  db.delete(entry.getKey());
                  count++;

                  if (count > configuration.clearThreshold()) {
                     destroyDatabase = true;
                     break;
                  }
               }
            } finally {
               try {
                  it.close();
               } catch (IOException e) {
                  log.warnUnableToCloseDbIterator(e);
               }
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
         } catch (IOException e) {
            throw new PersistenceException(e);
         }
      }
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

   @SuppressWarnings("unchecked")
   @Override
   public void process(KeyFilter keyFilter, CacheLoaderTask cacheLoaderTask, Executor executor, boolean loadValues, boolean loadMetadata) {

      int batchSize = 100;
      ExecutorAllCompletionService eacs = new ExecutorAllCompletionService(executor);
      final TaskContext taskContext = new TaskContextImpl();

      List<Map.Entry<byte[], byte[]>> entries = new ArrayList<Map.Entry<byte[], byte[]>>(batchSize);
      try {
         semaphore.acquire();
      } catch (InterruptedException e) {
         throw new PersistenceException("Cannot acquire semaphore: CacheStore is likely stopped.", e);
      }
      try {
         if (stopped) {
            throw new PersistenceException("LevelDB is stopped");
         }
         DBIterator it = db.iterator(new ReadOptions().fillCache(false));
         try {
            for (it.seekToFirst(); it.hasNext(); ) {
               Map.Entry<byte[], byte[]> entry = it.next();
               entries.add(entry);
               if (entries.size() == batchSize) {
                  final List<Map.Entry<byte[], byte[]>> batch = entries;
                  entries = new ArrayList<Map.Entry<byte[], byte[]>>(batchSize);
                  submitProcessTask(cacheLoaderTask, keyFilter, eacs, taskContext, batch, loadValues, loadMetadata);
               }
            }
            if (!entries.isEmpty()) {
               submitProcessTask(cacheLoaderTask, keyFilter, eacs, taskContext, entries, loadValues, loadMetadata);
            }

            eacs.waitUntilAllCompleted();
            if (eacs.isExceptionThrown()) {
               throw new PersistenceException("Execution exception!", eacs.getFirstException());
            }
         } catch (Exception e) {
            throw new PersistenceException(e);
         } finally {
            try {
               it.close();
            } catch (IOException e) {
               log.warnUnableToCloseDbIterator(e);
            }
         }
      } finally {
         semaphore.release();
      }
   }

   @SuppressWarnings("unchecked")
   private void submitProcessTask(final CacheLoaderTask cacheLoaderTask, final KeyFilter filter, CompletionService ecs,
                                  final TaskContext taskContext, final List<Map.Entry<byte[], byte[]>> batch,
                                  final boolean loadValues, final boolean loadMetadata) {
      ecs.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            try {
               long now = ctx.getTimeService().wallClockTime();
               for (Map.Entry<byte[], byte[]> pair : batch) {
                  if (taskContext.isStopped()) {break;}
                  Object key = unmarshall(pair.getKey());
                  if (filter == null || filter.accept(key)) {
                     MarshalledEntry entry = loadValues || loadMetadata ? (MarshalledEntry) unmarshall(pair.getValue()) : null;
                     boolean isExpired = entry != null && entry.getMetadata() != null && entry.getMetadata().isExpired(now);
                     if (!isExpired) {
                        if (!loadValues || !loadMetadata) {
                           entry = ctx.getMarshalledEntryFactory().newMarshalledEntry(
                                 key, loadValues ? entry.getValue() : null, loadMetadata ? entry.getMetadata() : null);
                        }
                        cacheLoaderTask.processEntry(entry, taskContext);
                     }
                  }
               }
            } catch (Exception e) {
               log.errorExecutingParallelStoreTask(e);
               throw e;
            }
            return null;
         }
      });
   }

   @Override
   public boolean delete(Object key)  {
      try {
         byte[] keyBytes = marshall(key);
         semaphore.acquire();
         try {
            if (stopped) {
               throw new PersistenceException("LevelDB is stopped");
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
   public void write(MarshalledEntry me)  {
      try {
         byte[] marshelledKey = marshall(me.getKey());
         byte[] marshalledEntry = marshall(me);
         semaphore.acquire();
         try {
            if (stopped) {
               throw new PersistenceException("LevelDB is stopped");
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
   public MarshalledEntry load(Object key)  {
      try {
         byte[] marshalledEntry;
         semaphore.acquire();
         try {
            if (stopped) {
               throw new PersistenceException("LevelDB is stopped");
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
            throw new PersistenceException("LevelDB is stopped");
         }
         // Drain queue and update expiry tree
         List<ExpiryEntry> entries = new ArrayList<ExpiryEntry>();
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
                  List<Object> al = new ArrayList<Object>(2);
                  al.add(existing);
                  al.add(entry.key);
                  expiredDb.put(expiryBytes, marshall(al));
               }
            } else {
               expiredDb.put(expiryBytes, keyBytes);
            }
         }

         List<Long> times = new ArrayList<Long>();
         List<Object> keys = new ArrayList<Object>();
         DBIterator it = expiredDb.iterator(new ReadOptions().fillCache(false));
         long now = ctx.getTimeService().wallClockTime();
         try {
            for (it.seekToFirst(); it.hasNext();) {
               Map.Entry<byte[], byte[]> entry = it.next();

               Long time = (Long) unmarshall(entry.getKey());
               if (time > now)
                  break;
               times.add(time);
               Object key = unmarshall(entry.getValue());
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
         } finally {
            try {
               it.close();
            } catch (IOException e) {
               log.warnUnableToCloseDbIterator(e);
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
         // Coding getExpiryTime() for transient entries has the risk of
         // being a moving target
         // which could lead to unexpected results, hence, InternalCacheEntry
         // calls are required
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

}
