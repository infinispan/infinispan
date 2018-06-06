package org.infinispan.persistence.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.commons.util.ByRef;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.concurrent.CompletableFutures;

public class OrderedUpdatesManagerImpl implements OrderedUpdatesManager {
   @Inject private InternalDataContainer<Object, Object> dataContainer;
   @Inject private DistributionManager distributionManager;
   @Inject private PersistenceManager persistenceManager;

   private ConcurrentHashMap<Object, CompletableFuture<?>> locks = new ConcurrentHashMap<>();

   @Override
   public CompletableFuture<?> waitFuture(Object key) {
      return locks.get(key);
   }

   private void lock(Object key, ByRef<CompletableFuture<?>> lockedFuture, ByRef<CompletableFuture<?>> waitFuture) {
      CompletableFuture<?> myFuture = new CompletableFuture<>();
      CompletableFuture<?> prevFuture = locks.putIfAbsent(key, myFuture);
      if (prevFuture == null) {
         lockedFuture.set(myFuture);
      } else {
         waitFuture.set(prevFuture);
      }
   }

   @Override
   public CompletableFuture<Void> checkLockAndStore(Object key, EntryVersion version, Function<CompletableFuture<?>, CompletableFuture<?>> enableTimeout, Consumer<Object> store) {
      ByRef<CompletableFuture<?>> lockedFuture = new ByRef<>(null);
      ByRef<CompletableFuture<?>> waitFuture = new ByRef<>(null);
      dataContainer.compute(key, (k, oldEntry, factory) -> {
         // if there is no entry in DC, the value was either already passivated or removed by a subsequent
         // removal, or the command did not write anything (that shouldn't happen as we check if the command is successful)
         if (oldEntry == null) {
            return null;
         }
         Metadata oldMetadata;
         EntryVersion oldVersion;
         if ((oldMetadata = oldEntry.getMetadata()) == null || (oldVersion = oldMetadata.version()) == null) {
            // the record was written without version?
            lock(k, lockedFuture, waitFuture);
         } else {
            InequalVersionComparisonResult result = oldVersion.compareTo(version);
            switch (result) {
               case AFTER:
                  // just ignore the write
                  break;
               case EQUAL:
                  lock(k, lockedFuture, waitFuture);
                  break;
               case BEFORE: // the actual version was not committed but we're here?
               case CONFLICTING: // not used with numeric versions
               default:
                  throw new IllegalStateException("DC version: " + oldVersion + ", cmd version " + version);
            }
         }
         return oldEntry;
      });
      CompletableFuture<?> wf = waitFuture.get();
      if (wf != null) {
         return enableTimeout.apply(wf).thenCompose(nil -> checkLockAndStore(key, version, enableTimeout, store));
      }
      CompletableFuture<?> lf = lockedFuture.get();
      if (lf != null) {
         try {
            store.accept(key);
         } finally {
            if (!locks.remove(key, lf)) {
               throw new IllegalStateException("No one but me should be able to replace the future");
            }
            lf.complete(null);
         }
      }
      return null;
   }

   @Override
   public CompletableFuture<?> invalidate(Object[] keys) {
      // We don't care about any timeouts; if the locking takes too long the sender gets replication time out
      // and resends the invalidation - that doesn't cause any harm
      List<CompletableFuture<?>> futures = null;
      for (int i = 0; i < keys.length; ++i) {
         Object key = keys[i];
         if (key == null) break;
         CompletableFuture<?> future = checkLockAndRemove(key);
         if (future != null && !future.isDone()) {
            if (futures == null) {
               futures = new ArrayList<>();
            }
            futures.add(future);
         }
      }
      if (futures == null) {
         return CompletableFutures.completedNull();
      } else if (futures.size() == 1) {
         return futures.get(0);
      } else {
         return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
      }
   };

   private CompletableFuture<?> checkLockAndRemove(Object key) {
      ByRef<CompletableFuture<?>> lockedFuture = new ByRef<>(null);
      ByRef<CompletableFuture<?>> waitFuture = new ByRef<>(null);
      dataContainer.compute(key, (k, oldEntry, factory) -> {
         // if the entry is not null, the entry was not invalidated, or it was already written again
         if (oldEntry == null) {
            lock(k, lockedFuture, waitFuture);
         }
         return oldEntry;
      });
      CompletableFuture<?> wf = waitFuture.get();
      if (wf != null) {
         return wf.thenCompose(nil -> checkLockAndRemove(key));
      }
      CompletableFuture<?> lf = lockedFuture.get();
      if (lf != null) {
         try {
            DistributionInfo info = distributionManager.getCacheTopology().getDistribution(key);
            PersistenceManager.AccessMode mode = info.isPrimary() ?
                  PersistenceManager.AccessMode.BOTH : PersistenceManager.AccessMode.PRIVATE;
            persistenceManager.deleteFromAllStores(key, mode);
         } finally {
            if (!locks.remove(key, lf)) {
               throw new IllegalStateException("No one but me should be able to replace the future");
            }
            lf.complete(null);
         }
      }
      return null;
   }

}
