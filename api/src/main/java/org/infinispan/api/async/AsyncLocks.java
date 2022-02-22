package org.infinispan.api.async;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.configuration.LockConfiguration;

/**
 * @since 14.0
 **/
public interface AsyncLocks {
   CompletionStage<AsyncLock> create(String name, LockConfiguration configuration);

   CompletionStage<AsyncLock> lock(String name);

   CompletionStage<Void> remove(String name);

   Flow.Publisher<String> names();
}
