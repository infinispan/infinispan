package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS;
import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.gracefulShutdown;
import static org.infinispan.persistence.remote.upgrade.HotRodMigratorHelper.migrateEntriesWithMetadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class MigrationTask implements DistributedCallable<Object, Object, Integer> {

   private static final Log log = LogFactory.getLog(MigrationTask.class, Log.class);

   private final Set<Integer> segments;
   private final int readBatch;
   private final int threads;
   private volatile Cache<Object, Object> cache;

   public MigrationTask(Set<Integer> segments, int readBatch, int threads) {
      this.segments = segments;
      this.readBatch = readBatch;
      this.threads = threads;
   }

   @Override
   public Integer call() throws Exception {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      PersistenceManager loaderManager = cr.getComponent(PersistenceManager.class);
      Set<RemoteStore> stores = loaderManager.getStores(RemoteStore.class);
      Marshaller marshaller = new MigrationMarshaller();
      byte[] knownKeys;
      try {
         knownKeys = marshaller.objectToByteBuffer(MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS);
      } catch (Exception e) {
         throw new CacheException(e);
      }
      Iterator<RemoteStore> storeIterator = stores.iterator();
      if (storeIterator.hasNext()) {
         RemoteStore store = storeIterator.next();
         final RemoteCache<Object, Object> storeCache = store.getRemoteCache();
         RemoteStoreConfiguration storeConfig = store.getConfiguration();
         if (!storeConfig.hotRodWrapping()) {
            throw log.remoteStoreNoHotRodWrapping(cache.getName());
         }
         final AtomicInteger count = new AtomicInteger(0);
         ExecutorService es = Executors.newFixedThreadPool(threads);
         migrateEntriesWithMetadata(storeCache, cache, es, knownKeys, count, segments, readBatch);
         gracefulShutdown(es);
         return count.get();
      }
      return null;
   }

   @Override
   public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
      this.cache = cache;
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
