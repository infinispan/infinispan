package org.infinispan.persistence.manager;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.container.versioning.EntryVersion;

public interface OrderedUpdatesManager {
   CompletableFuture<?> waitFuture(Object key);

   CompletableFuture<?> checkLockAndStore(Object key, EntryVersion version,
                                          Function<CompletableFuture<?>, CompletableFuture<?>> enableTimeout,
                                          Consumer<Object> store);

   CompletableFuture<?> invalidate(Object[] keys);
}
