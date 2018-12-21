package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS;
import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.awaitTermination;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.ConcurrentHashSet;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class MigrationTask implements DistributedCallable<Object, Object, Integer> {

   private static final Log log = LogFactory.getLog(MigrationTask.class, Log.class);

   private static final String THREAD_NAME = "RollingUpgrade-MigrationTask";

   private final Set<Integer> segments;
   private final int readBatch;
   private final int threads;
   private final ConcurrentHashSet<WrappedByteArray> deletedKeys = new ConcurrentHashSet<>();
   private byte[] ignoredKey;

   private transient Set<RemoteStore> stores;
   private transient Cache<Object, Object> cache;
   private transient ExecutorService executorService;
   private transient final RemoveListener listener = new RemoveListener();

   public MigrationTask(Set<Integer> segments, int readBatch, int threads) {
      this.segments = segments;
      this.readBatch = readBatch;
      this.threads = threads;
   }

   @Listener(clustered = true)
   @SuppressWarnings("unused")
   private class RemoveListener {
      @CacheEntryRemoved
      public void entryRemoved(CacheEntryRemovedEvent event) {
         deletedKeys.add(new WrappedByteArray((byte[]) event.getKey()));
      }
   }

   @Override
   public Integer call() throws Exception {
      try {
         Iterator<RemoteStore> storeIterator = stores.iterator();
         if (storeIterator.hasNext()) {
            RemoteStore store = storeIterator.next();
            RemoteCache<Object, Object> storeCache = store.getRemoteCache();
            RemoteStoreConfiguration storeConfig = store.getConfiguration();
            if (!storeConfig.hotRodWrapping()) {
               throw log.remoteStoreNoHotRodWrapping(cache.getName());
            }
            AtomicInteger counter = new AtomicInteger(0);
            migrateEntriesWithMetadata(storeCache, counter);
            awaitTermination(executorService);
            return counter.intValue();
         }
         return null;
      } finally {
         cache.removeListener(listener);
         if (executorService != null) {
            executorService.shutdownNow();
         }
      }
   }

   private void migrateEntriesWithMetadata(RemoteCache<Object, Object> sourceCache, AtomicInteger counter) {
      try (CloseableIterator<Map.Entry<Object, MetadataValue<Object>>> iterator = sourceCache.retrieveEntriesWithMetadata(segments, readBatch)) {
         while (iterator.hasNext() && !Thread.currentThread().isInterrupted()) {
            Map.Entry<Object, MetadataValue<Object>> entry = iterator.next();
            if (!Arrays.equals((byte[]) entry.getKey(), ignoredKey)) {
               MetadataValue<Object> metadataValue = entry.getValue();
               int lifespan = metadataValue.getLifespan();
               int maxIdle = metadataValue.getMaxIdle();
               long version = metadataValue.getVersion();
               Metadata metadata = new EmbeddedMetadata.Builder()
                     .version(new NumericVersion(version))
                     .lifespan(lifespan, TimeUnit.SECONDS)
                     .maxIdle(maxIdle, TimeUnit.SECONDS)
                     .build();
               executorService.submit(() -> {
                  Object key = entry.getKey();
                  if (!deletedKeys.contains(new WrappedByteArray((byte[]) key))) {
                     InternalMetadata internalMetadata = new InternalMetadataImpl(metadata, metadataValue.getCreated(), metadataValue.getLastUsed());
                     cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD, Flag.ROLLING_UPGRADE).putIfAbsent(entry.getKey(), entry.getValue().getValue(), internalMetadata);
                  }
                  int currentCount = counter.incrementAndGet();
                  if (log.isDebugEnabled() && currentCount % 100 == 0)
                     log.debugf(">>    Migrated %s entries\n", currentCount);
               });
            }
         }
      }
   }

   @Override
   public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      PersistenceManager loaderManager = cr.getComponent(PersistenceManager.class);
      this.stores = loaderManager.getStores(RemoteStore.class);
      Marshaller marshaller = new MigrationMarshaller(cache.getCacheManager().getClassWhiteList());
      this.cache = cache;
      this.cache.addFilteredListener(listener, new RemovedFilter<>(), null, Util.asSet(CacheEntryRemoved.class));
      DefaultThreadFactory threadFactory = new DefaultThreadFactory(null, 1, THREAD_NAME + "-%t", null, null);
      this.executorService = Executors.newFixedThreadPool(threads, threadFactory);
      try {
         this.ignoredKey = marshaller.objectToByteBuffer(MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS);
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   public static class Externalizer extends AbstractExternalizer<MigrationTask> {

      @Override
      public Set<Class<? extends MigrationTask>> getTypeClasses() {
         return Collections.singleton(MigrationTask.class);
      }

      @Override
      public void writeObject(ObjectOutput output, MigrationTask task) throws IOException {
         UnsignedNumeric.writeUnsignedInt(output, task.readBatch);
         UnsignedNumeric.writeUnsignedInt(output, task.threads);
         BitSet bs = new BitSet();
         for (Integer segment : task.segments) {
            bs.set(segment);
         }
         byte[] bytes = bs.toByteArray();
         UnsignedNumeric.writeUnsignedInt(output, bytes.length);
         output.write(bs.toByteArray());
      }

      @Override
      public MigrationTask readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int readBatch = UnsignedNumeric.readUnsignedInt(input);
         int threads = UnsignedNumeric.readUnsignedInt(input);
         int segmentsSize = UnsignedNumeric.readUnsignedInt(input);
         byte[] bytes = new byte[segmentsSize];
         input.read(bytes);
         BitSet bitSet = BitSet.valueOf(bytes);
         Set<Integer> segments = bitSet.stream().boxed().collect(Collectors.toSet());
         return new MigrationTask(segments, readBatch, threads);
      }
   }

}
