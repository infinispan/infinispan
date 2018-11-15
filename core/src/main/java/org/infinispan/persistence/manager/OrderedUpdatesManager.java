package org.infinispan.persistence.manager;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.container.versioning.EntryVersion;

public interface OrderedUpdatesManager {
   CompletableFuture<?> waitFuture(Object key);

   CompletionStage<Void> checkLockAndStore(Object key, EntryVersion version,
                                          Function<CompletableFuture<?>, CompletableFuture<?>> enableTimeout,
                                          Function<Object, CompletionStage<Void>> store);

   CompletionStage<?> invalidate(Object[] keys);
}
